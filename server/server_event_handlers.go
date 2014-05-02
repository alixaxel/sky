package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/skydb/sky/db"
)

func (s *Server) addEventHandlers() {
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.getEventsHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.deleteEventsHandler(w, req, params)
	}).Methods("DELETE")

	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.getEventHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.insertEventHandler(w, req, params)
	}).Methods("PUT")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.insertEventHandler(w, req, params)
	}).Methods("PATCH")
	s.ApiHandleFunc("/tables/{name}/objects/{objectId}/events/{timestamp}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.deleteEventHandler(w, req, params)
	}).Methods("DELETE")

	// Streaming import.
	s.router.HandleFunc("/tables/{name}/events", s.streamUpdateEventsHandler).Methods("PATCH")
}

// GET /tables/:name/objects/:objectId/events
func (s *Server) getEventsHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	// Retrieve raw events.
	var events []*db.Event
	err = t.View(func(tx *db.Tx) error {
		var err error
		events, err = tx.Events(vars["objectId"])
		return err
	})
	if events == nil {
		events = make([]*db.Event, 0)
	}
	return events, err
}

// DELETE /tables/:name/objects/:objectId/events
func (s *Server) deleteEventsHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}
	err = t.Update(func(tx *db.Tx) error {
		return tx.DeleteEvents(vars["objectId"])
	})
	return nil, err
}

// GET /tables/:name/objects/:objectId/events/:timestamp
func (s *Server) getEventHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	// Parse timestamp.
	timestamp, err := time.Parse(time.RFC3339, vars["timestamp"])
	if err != nil {
		return nil, err
	}

	// Find event.
	var event *db.Event
	err = t.View(func(tx *db.Tx) error {
		var err error
		event, err = tx.Event(vars["objectId"], timestamp)
		return err
	})
	return event, err
}

// PUT /tables/:name/objects/:objectId/events/:timestamp
func (s *Server) insertEventHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	timestamp, err := time.Parse(time.RFC3339, vars["timestamp"])
	if err != nil {
		return nil, err
	}
	data, _ := params["data"].(map[string]interface{})

	err = t.Update(func(tx *db.Tx) error {
		var event = &db.Event{
			Timestamp: timestamp,
			Data:      data,
		}
		return tx.InsertEvent(vars["objectId"], event)
	})
	return nil, err
}

// Used by streaming handler.
type objectEvents map[string][]*db.Event

func (o objectEvents) Count() int {
	var count int
	for _, events := range o {
		count += len(events)
	}
	return count
}

func (s *Server) flushTableEvents(table *db.Table, objects objectEvents) error {
	return table.Update(func(tx *db.Tx) error {
		return tx.InsertObjects(objects)
	})
}

// PATCH /tables/:name/events
func (s *Server) streamUpdateEventsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t0 := time.Now()
	tableName := vars["name"]

	table, err := s.OpenTable(tableName)
	if err != nil {
		s.logger.Printf("ERR %v", err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"message":"%v"}`, err)
		return
	}

	// Check for flush threshold/buffer passed as URL params.
	flushThreshold := s.streamFlushThreshold
	if rawFlushThreshold := req.FormValue("flush-threshold"); rawFlushThreshold != "" {
		threshold, err := strconv.Atoi(rawFlushThreshold)
		if err == nil {
			flushThreshold = uint(threshold)
		} else {
			s.logger.Printf("ERR: invalid flush-threshold parameter: %v", err)
			fmt.Fprintf(w, `{"message":"invalid flush-threshold parameter: %v"}`, err)
			return
		}
	}

	// GOROUTINE for event decoding
	decodeEvents := make(chan *eventMessage, 2*flushThreshold) // decoded events
	decodeErrors := make(chan error)                           // decoding errors
	decodeStop := make(chan struct{})                          // signal the decoder to stop (by closing the channel)
	go func(source io.Reader, events chan<- *eventMessage, errors chan<- error, stop <-chan struct{}) {
		decoder := json.NewDecoder(source)
		for {
			select {
			case <-stop:
				return
			default:
				event := &eventMessage{}
				err := decoder.Decode(&event)
				switch {
				case err == io.EOF:
					// signal end of event stream
					close(events)
					return
				case err != nil:
					errors <- fmt.Errorf("Malformed json event: %v", err)
					return
				default:
					events <- event
				}
			}
		}
	}(req.Body, decodeEvents, decodeErrors, decodeStop)

	// total events successfully written
	// note that the count is updated by the flushing goroutine only!
	// any read access must wait until that goroutine finishes
	eventsWritten := 0

	// GOROUTINE for event flushing
	writeEvents := make(chan objectEvents, 1000)
	writeErrors := make(chan error)
	go func(events <-chan objectEvents, errors chan<- error) {
		for batch := range events {
			start := time.Now()
			if err := s.flushTableEvents(table, batch); err != nil {
				errors <- err
				return
			}
			elapsed := time.Since(start).Seconds()
			count := batch.Count()
			eventsWritten += count
			s.logger.Printf("[STREAM][FLUSH] client=`%s`, table=`%s`, count=`%d`, duration=`%f`", req.RemoteAddr, tableName, count, elapsed)
		}
		// signal finishing successfully
		close(errors)
	}(writeEvents, writeErrors)

	eventsByObject := make(objectEvents)

	// Main loop receives decoded events,
	// deserializes, factorizes and batches them for writing
	err = func() error {
		var err error
		var flushEventCount uint
	loop:
		for {
			var event *eventMessage
			select {
			case e, ok := <-decodeEvents:
				if ok {
					event = e
				} else {
					break loop
				}
			case err = <-decodeErrors:
				break loop
			case err = <-writeErrors:
				// make sure we stop the decoder
				close(decodeStop)
				return err // skip flushing remaining events
			}

			// Extract the object identifier.
			var objectId = event.ID
			if objectId == "" {
				err = fmt.Errorf("Object identifier required")
				break loop
			}

			// Add event to the batch.
			eventsByObject[objectId] = append(eventsByObject[objectId], &db.Event{Timestamp: event.Timestamp, Data: event.Data})
			flushEventCount++

			// Flush events if exceeding threshold.
			if flushEventCount >= flushThreshold {
				writeEvents <- eventsByObject
				eventsByObject = make(objectEvents)
				flushEventCount = 0
			}
		}
		// make sure we stop the decoder
		close(decodeStop)
		// Flush remaining events
		if len(eventsByObject) > 0 {
			writeEvents <- eventsByObject
		}
		close(writeEvents)
		// Wait for flushing to finish.
		// If the flush fails, report that error instead of any other,
		// because flush failures involve an event that was successfully decoded,
		// thus logically preceeding any decode/deserialize error
		if err2 := <-writeErrors; err != nil {
			err = err2
		}
		return err
	}()

	if err != nil {
		s.logger.Printf("ERR %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message":"%v", "events_written":%v}`, err, eventsWritten)
		return
	}

	fmt.Fprintf(w, `{"events_written":%v}`, eventsWritten)

	s.logger.Printf("[STREAM][CLOSE] client=`%s`, table=`%s`, count=`%d`, duration=`%0.3f`", req.RemoteAddr, tableName, eventsWritten, time.Since(t0).Seconds())
}

type eventMessage struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// DELETE /tables/:name/objects/:objectId/events/:timestamp
func (s *Server) deleteEventHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (ret interface{}, err error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	timestamp, err := time.Parse(time.RFC3339, vars["timestamp"])
	if err != nil {
		return nil, fmt.Errorf("Unable to parse timestamp: %v", timestamp)
	}

	return nil, t.Update(func(tx *db.Tx) error {
		return tx.DeleteEvent(vars["objectId"], timestamp)
	})
}
