package query

import (
	"fmt"

	"github.com/skydb/sky/db"
)

// A Variable represents a variable declaration on the cursor. The value
// of the variable persist for the duration of an object and can be
// referenced like any other property on the database. The variable can
// also be associated with another variable for the purpose of reusing
// factorization.
type Variable struct {
	queryElementImpl
	Name        string
	DataType    string
	Association string
	PropertyId  int64
}

func NewVariable(name string, dataType string) *Variable {
	return &Variable{Name: name, DataType: dataType}
}

// Determines if the variable is a system variable.
func (v *Variable) IsSystemVariable() bool {
	return (len(v.Name) != 0 && v.Name[0] == '@')
}

// Returns the C data type used by this variable.
func (v *Variable) cType() string {
	switch v.DataType {
	case db.String:
		return "sky_string_t"
	case db.Factor, db.Integer:
		return "int32_t"
	case db.Float:
		return "double"
	case db.Boolean:
		return "bool"
	}
	panic(fmt.Sprintf("Invalid data type: %v", v.DataType))
}

func (v *Variable) Codegen() (string, error) {
	return "", nil
}

func (v *Variable) String() string {
	var dataType string
	switch v.DataType {
	case db.Factor:
		dataType = "FACTOR"
	case db.String:
		dataType = "STRING"
	case db.Integer:
		dataType = "INTEGER"
	case db.Float:
		dataType = "FLOAT"
	case db.Boolean:
		dataType = "BOOLEAN"
	}

	str := fmt.Sprintf("DECLARE @%s AS %s", v.Name, dataType)
	if v.Association != "" {
		str += "(@" + v.Association + ")"
	}
	return str
}
