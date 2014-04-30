package server

import (
	"net/http"
	"sort"

	"github.com/gorilla/mux"
	"github.com/skydb/sky/db"
)

func (s *Server) addPropertyHandlers() {
	s.ApiHandleFunc("/tables/{name}/properties", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.getPropertiesHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/properties", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.createPropertyHandler(w, req, params)
	}).Methods("POST")

	s.ApiHandleFunc("/tables/{name}/properties/{propertyName}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.getPropertyHandler(w, req, params)
	}).Methods("GET")
	s.ApiHandleFunc("/tables/{name}/properties/{propertyName}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.updatePropertyHandler(w, req, params)
	}).Methods("PATCH")
	s.ApiHandleFunc("/tables/{name}/properties/{propertyName}", func(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
		return s.deletePropertyHandler(w, req, params)
	}).Methods("DELETE")
}

// GET /tables/:name/properties
func (s *Server) getPropertiesHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)

	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	var properties = make(db.PropertySlice, 0)
	table.View(func(tx *db.Tx) error {
		propertiesById, _ := tx.Properties()
		for _, p := range propertiesById {
			properties = append(properties, p)
		}
		return nil
	})
	sort.Sort(properties)
	return properties, nil
}

// POST /tables/:name/properties
func (s *Server) createPropertyHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	name, _ := params["name"].(string)
	transient, _ := params["transient"].(bool)
	dataType, _ := params["dataType"].(string)

	var p *db.Property
	err = table.Update(func(tx *db.Tx) error {
		var err error
		p, err = tx.CreateProperty(name, dataType, transient)
		return err
	})
	return p, err
}

// GET /tables/:name/properties/:propertyName
func (s *Server) getPropertyHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	var p *db.Property
	err = table.View(func(tx *db.Tx) error {
		var err error
		p, err = tx.Property(vars["propertyName"])
		return err
	})
	return p, err
}

// PATCH /tables/:name/properties/:propertyName
func (s *Server) updatePropertyHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}
	name, _ := params["name"].(string)

	var p *db.Property
	err = table.Update(func(tx *db.Tx) error {
		var err error
		p, err = tx.RenameProperty(vars["propertyName"], name)
		return err
	})
	return p, err
}

// DELETE /tables/:name/properties/:propertyName
func (s *Server) deletePropertyHandler(w http.ResponseWriter, req *http.Request, params map[string]interface{}) (interface{}, error) {
	vars := mux.Vars(req)
	table, err := s.OpenTable(vars["name"])
	if err != nil {
		return nil, err
	}

	return nil, table.Update(func(tx *db.Tx) error {
		return tx.DeleteProperty(vars["propertyName"])
	})
}
