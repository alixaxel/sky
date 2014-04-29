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

	table, err := s.OpenTable(vars["name"])
	if err != nil {
		s.logger.Printf("ERR %v", err)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"message":"%v"}`, err)
		return
	}

	// Check for flush threshold/buffer passed as URL params.
	flushThreshold := s.StreamFlushThreshold
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

	eventsByObject := make(objectEvents)
	flushEventCount := uint(0)

	eventsWritten := 0
	err = func() error {
		// Stream in JSON event objects.
		decoder := json.NewDecoder(req.Body)

		// Set up events decoder listener
		events := make(chan *eventMessage)
		eventErrors := make(chan error)
		go func(decoder *json.Decoder) {
			for {
				event := &eventMessage{}
				if err := decoder.Decode(&event); err == io.EOF {
					close(events)
					break
				} else if err != nil {
					eventErrors <- fmt.Errorf("Malformed json event: %v", err)
					break
				}
				events <- event
			}
		}(decoder)

	loop:
		for {

			// Read in a JSON object.
			var msg *eventMessage
			select {
			case e, ok := <-events:
				if !ok {
					break loop
				} else {
					msg = e
				}

			case err := <-eventErrors:
				return err
			}

			// Extract the object identifier.
			var objectId = msg.ID
			if objectId == "" {
				return fmt.Errorf("Object identifier required")
			}

			// Add event to table buffer.
			eventsByObject[objectId] = append(eventsByObject[objectId], &db.Event{Timestamp: msg.Timestamp, Data: msg.Data})
			flushEventCount++

			// Flush events if exceeding threshold.
			if flushEventCount >= flushThreshold {
				flushedCount := 0
				s.logger.Printf("[STREAM] [FLUSH THRESHOLD] [FLUSHING] threshold=`%d` events=`%d`", flushThreshold, flushEventCount)
				if err := s.flushTableEvents(table, eventsByObject); err != nil {
					return err
				}
				eventsWritten += eventsByObject.Count()
				flushedCount += eventsByObject.Count()
				s.logger.Printf("[STREAM] [FLUSH THRESHOLD] [FLUSHED] events=`%d`", flushedCount)

				eventsByObject = make(objectEvents)
				flushEventCount = 0
			}

		}

		return nil
	}()

	// Flush all events before closing the stream.
	if err == nil {
		err = func() error {
			flushedCount := 0
			s.logger.Printf("[STREAM] [END FLUSH] [FLUSHING] events=`%d`")
			if err := s.flushTableEvents(table, eventsByObject); err != nil {
				return err
			}
			eventsWritten += eventsByObject.Count()
			flushedCount += eventsByObject.Count()
			s.logger.Printf("[STREAM] [END FLUSH] [FLUSHED] events=`%d`", flushedCount)
			return nil
		}()
	}

	if err != nil {
		s.logger.Printf("ERR %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"message":"%v", "events_written":%v}`, err, eventsWritten)
		return
	}

	fmt.Fprintf(w, `{"events_written":%v}`, eventsWritten)

	s.logger.Printf("%s \"%s %s %s %d events OK\" %0.3f", req.RemoteAddr, req.Method, req.URL.Path, req.Proto, eventsWritten, time.Since(t0).Seconds())
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
