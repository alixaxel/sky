package db

import (
	"fmt"
	"regexp"
)

// Property represents part of the schema for a Table.
type Property struct {
	table     *Table
	ID        int    `json:"id"`
	Name      string `json:"name"`
	DataType  string `json:"dataType"`
	Transient bool   `json:"transient"`
}

// Validate checks that the property is valid. Properties can be invalid if
// non-alphanumeric characters are used in its name or if the data type is not
// a valid type.
func (p *Property) Validate() error {
	// Validate that name is non-blank and doesn't contain invalid characters.
	if p.Name == "" || !regexp.MustCompile(`^\w+$`).MatchString(p.Name) {
		return fmt.Errorf("invalid property name: %s", p.Name)
	}

	// Validate data type.
	switch p.DataType {
	case Factor, String, Integer, Float, Boolean:
	default:
		return fmt.Errorf("invalid data type: %s", p.DataType)
	}

	return nil
}

// Clone makes a copy of the property.
func (p *Property) Clone() *Property {
	return &Property{
		table:     p.table,
		ID:        p.ID,
		Name:      p.Name,
		Transient: p.Transient,
		DataType:  p.DataType,
	}
}

// Cast converts a value into the appropriate Go type based on the property's data type.
func (p *Property) Cast(v interface{}) interface{} {
	if p.DataType == Factor || p.DataType == String {
		switch v := v.(type) {
		case string:
			return v
		default:
			return ""
		}
	} else if p.DataType == Integer {
		switch v := promote(v).(type) {
		case int64:
			return v
		case float64:
			return int64(v)
		default:
			return 0
		}
	} else if p.DataType == Float {
		switch v := promote(v).(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		default:
			return 0
		}
	} else if p.DataType == Boolean {
		switch v := v.(type) {
		case bool:
			return v
		default:
			return false
		}
	}
	return v
}

// PropertySlice represents a list of properties that can be sorted by ID.
type PropertySlice []*Property

func (s PropertySlice) Len() int           { return len(s) }
func (s PropertySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s PropertySlice) Less(i, j int) bool { return s[i].ID < s[j].ID }
