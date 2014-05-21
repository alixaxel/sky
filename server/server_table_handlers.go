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
