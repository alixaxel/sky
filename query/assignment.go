package query

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/skydb/sky/db"
)

// An Assignment statement sets a variable to a given value.
type Assignment struct {
	queryElementImpl
	target     *VarRef
	expression Expression
}

// Creates a new assignment.
func NewAssignment() *Assignment {
	return &Assignment{}
}

// Retrieves the function name used during codegen.
func (a *Assignment) FunctionName(init bool) string {
	return fmt.Sprintf("a%d", a.ElementId())
}

// Retrieves the merge function name used during codegen.
func (a *Assignment) MergeFunctionName() string {
	return ""
}

// Returns the target variable.
func (a *Assignment) Target() *VarRef {
	return a.target
}

// Sets the variable that will be assigned.
func (a *Assignment) SetTarget(value *VarRef) {
	if a.target != nil {
		a.target.SetParent(nil)
	}
	a.target = value
	if a.target != nil {
		a.target.SetParent(a)
	}
}

// Returns the expression to be evaluated.
func (a *Assignment) Expression() Expression {
	return a.expression
}

// Sets the expression to be evaluated.
func (a *Assignment) SetExpression(expression Expression) {
	if a.expression != nil {
		a.expression.SetParent(nil)
	}
	a.expression = expression
	if a.expression != nil {
		a.expression.SetParent(a)
	}
}

// Returns a list of variable references within this assignment.
func (a *Assignment) VarRefs() []*VarRef {
	refs := []*VarRef{}
	refs = append(refs, a.target.VarRefs()...)
	refs = append(refs, a.expression.VarRefs()...)
	return refs
}

// Returns a list of variables declared within this statement.
func (a *Assignment) Variables() []*Variable {
	return []*Variable{}
}

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes a query selection into an untyped map.
func (a *Assignment) Serialize() map[string]interface{} {
	return map[string]interface{}{
		"type":       TypeAssignment,
		"target":     a.target.value,
		"expression": a.expression.String(),
	}
}

// Decodes an assignment from an untyped map.
func (a *Assignment) Deserialize(obj map[string]interface{}) error {
	if obj == nil {
		return errors.New("Assignment: Unable to deserialize nil.")
	}
	if obj["type"] != TypeAssignment {
		return fmt.Errorf("Assignment: Invalid statement type: %v", obj["type"])
	}

	// Deserialize "target".
	if target, ok := obj["target"].(string); ok {
		a.SetTarget(&VarRef{value: target})
	} else {
		return fmt.Errorf("Assignment: Invalid target: %v", obj["target"])
	}

	// Deserialize "expression".
	if expression, ok := obj["expression"].(string); ok {
		expr, err := NewExpressionParser().ParseString(expression)
		if err != nil {
			return err
		}
		a.SetExpression(expr)
	} else {
		return fmt.Errorf("Assignment: Invalid expression: %v", obj["expression"])
	}

	return nil
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates Lua code for the selection aggregation.
func (a *Assignment) CodegenAggregateFunction(init bool) (string, error) {
	variable, err := a.target.Variable()
	if variable == nil {
		return "", fmt.Errorf("Assignment: %s", err)
	}
	if variable.DataType == db.String {
		return "", fmt.Errorf("Assignment: String assignment is not currently supported.")
	}

	buffer := new(bytes.Buffer)

	fmt.Fprintf(buffer, "-- %s\n", a.String())
	fmt.Fprintf(buffer, "function %s(cursor, data)\n", a.FunctionName(init))

	targetCode, err := a.target.CodegenRaw()
	if err != nil {
		return "", err
	}
	expressionCode, err := a.expression.Codegen()
	if err != nil {
		return "", err
	}
	fmt.Fprintf(buffer, "  %s = %s\n", targetCode, expressionCode)

	fmt.Fprintln(buffer, "end")

	return buffer.String(), nil
}

// Generates Lua code for the selection merge.
func (a *Assignment) CodegenMergeFunction(fields map[string]interface{}) (string, error) {
	return "", nil
}

func (a *Assignment) Defactorize(data interface{}) error {
	return nil
}

func (a *Assignment) Finalize(data interface{}) error {
	return nil
}

func (a *Assignment) RequiresInitialization() bool {
	return false
}

// Converts the statement to a string-based representation.
func (a *Assignment) String() string {
	return fmt.Sprintf("SET @%s = %s", a.target.value, a.expression.String())
}
