package server

import (
	"errors"
	"net/http"
	"sort"

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
	return &tableMessage{table.Name()}, nil
}

// DELETE /tables/:name
func (s *Server) deleteTableHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	tableName := vars["name"]
	return nil, s.DB.DropTable(tableName)
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

type tableMessage struct {
	Name string `json:"name"`
}
