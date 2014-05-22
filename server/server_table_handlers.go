package server

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/boltdb/boltd"
	"github.com/gorilla/mux"
	"github.com/skydb/sky/db"
)

func (s *Server) addTableHandlers() {
	s.ApiHandleFunc("/tables", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.getTablesHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.getTableHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.createTableHandler(w, req, params)
	}).Methods("POST")
	s.ApiHandleFunc("/tables/{name}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.deleteTableHandler(w, req, params)
	}).Methods("DELETE")
	s.ApiHandleFunc("/tables/{name}/keys", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.tableKeysHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/stats", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.statsHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/top", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.objectStatsHandler(w, req, params)
	}).Methods("GET")
	s.router.HandleFunc("/tables/{name}/view/{path:.+}", s.viewTableHandler).Methods("GET")
	s.router.HandleFunc("/tables/{name}/copy", s.tableCopyHandler).Methods("GET")
}

// GET /tables
func (s *Server) getTablesHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	var tables, err = s.Tables()
	if err != nil {
		return nil, err
	}

	var messages = make([]*tableMessage, 0)
	for _, t := range tables {
		messages = append(messages, &tableMessage{t.Name()})
	}
	return messages, nil
}

// GET /tables/:name
func (s *Server) getTableHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}
	return &tableMessage{t.Name()}, nil
}

// POST /tables
func (s *Server) createTableHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	// Retrieve table parameters.
	name, ok := params["name"].(string)
	if !ok {
		return nil, errors.New("Table name required.")
	}

	// Return an error if the table already exists.
	table, err := s.DB.CreateTable(name, 16)
	if err != nil {
		return nil, err
	}

	// Create properties if present
	if properties, ok := params["properties"].([]interface{}); ok {
		table, err := s.OpenTable(name)
		if err != nil {
			return nil, err
		}
		err = table.Update(func(tx *db.Tx) error {
			for _, p := range properties {
				property, ok := p.(map[string]interface{})
				if !ok {
					return errors.New("Table property is not a valid map")
				}
				name, ok := property["name"].(string)
				if !ok {
					return errors.New("Table property name is not a string")
				}
				transient, ok := property["transient"].(bool)
				if !ok {
					return errors.New(fmt.Sprintf("Table property %s: transient is not a bool", name))
				}
				dataType, ok := property["dataType"].(string)
				if !ok {
					return errors.New(fmt.Sprintf("Table property %s: dataType is not a string", name))
				}
				if _, err := tx.CreateProperty(name, dataType, transient); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return &tableMessage{table.Name()}, nil
}

// DELETE /tables/:name
func (s *Server) deleteTableHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	tableName := vars["name"]
	return nil, s.DB.DropTable(tableName)
}

// GET /tables/:name/copy
func (s *Server) tableCopyHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	t.View(func(tx *db.Tx) error {
		w.Header().Set("Content-Length", strconv.Itoa(int(tx.Size())))
		w.Header().Set("Content-Type", "application/octet-steam")
		return tx.Copy(w)
	})
}

// GET /tables/:name/objects/keys
func (s *Server) tableKeysHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	var keys = make([]string, 0)
	_ = t.View(func(tx *db.Tx) error {
		for _, b := range tx.Shards() {
			b.ForEach(func(b, _ []byte) error {
				keys = append(keys, string(b))
				return nil
			})
		}
		return nil
	})
	sort.Strings(keys)

	return keys, nil
}

// GET /tables/:name/top?count=20&by=count
// Returns stats about top @count objects by count or size.
func (s *Server) objectStatsHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	var total int = 100
	if req.FormValue("count") != "" {
		i, err := strconv.Atoi(req.FormValue("count"))
		if err == nil {
			total = i
		}
	}

	var top = make([]struct {
		Id    string
		Count int
		Alloc int
		Inuse int
	}, total)

	var metric func(int) int // used to extract measured value from top entries by index
	var by string
	switch req.FormValue("by") {
	case "count":
		by = "count"
		metric = func(i int) int { return top[i].Count }
	default:
		by = "size"
		metric = func(i int) int { return top[i].Inuse }
	}

	_ = t.View(func(tx *db.Tx) error {
		var lowest int
		for _, shard := range tx.Shards() {
			shard.ForEach(func(key, val []byte) error {
				if val != nil {
					// If it's not a bucket, skip.
					return nil
				}
				// Get the stats for the object.
				var stats = shard.Bucket(key).Stats()

				// Extract the measured value.
				var value int
				switch {
				case by == "count":
					value = stats.KeyN
				case by == "size" && stats.InlineBucketN == 1:
					value = stats.InlineBucketInuse
				default:
					value = stats.LeafInuse
				}
				// If measured value reaches top, record the stats in the top list.
				if value > lowest {
					// Find the position in top list.
					i := sort.Search(total, func(i int) bool { return value > metric(i) })
					if !(i < total) {
						return nil // This shouldn't happen but let's be safe.
					}
					// Make room for new entry
					copy(top[i+1:], top[i:])
					top[i].Id = string(key)
					top[i].Count = stats.KeyN
					if stats.InlineBucketN == 1 {
						// For inline bucket report allocation as 0 since it's using parent's allocation.
						top[i].Alloc = 0
						top[i].Inuse = stats.InlineBucketInuse
					} else {
						top[i].Alloc = stats.LeafAlloc
						top[i].Inuse = stats.LeafInuse
					}
					// Update the lowest value.
					lowest = metric(total - 1)
				}
				return nil
			})
		}
		return nil
	})
	return top, nil
}

// GET /tables/:name/stats
func (s *Server) statsHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)

	// Return an error if the table already exists.
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	var all bool = req.FormValue("all") == "true"
	return table.Stats(all)
}

// GET /tables/:name/view
func (s *Server) viewTableHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t, err := s.OpenTable(vars["name"])
	if err != nil {
		http.Error(w, "table not found", http.StatusNotFound)
		return
	}

	prefix := fmt.Sprintf("/tables/%s/view", vars["name"])
	http.StripPrefix(prefix, boltd.NewHandler(t.DB())).ServeHTTP(w, req)
}

type tableMessage struct {
	Name string `json:"name"`
}
