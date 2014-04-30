package query

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/skydb/sky/db"
	"strconv"
	"strings"
)

// A selection statement aggregates data in a query.
type Selection struct {
	queryElementImpl
	Name       string
	Dimensions []string
	fields     []*SelectionField
}

// Creates a new selection.
func NewSelection() *Selection {
	return &Selection{}
}

// Retrieves the function name used during codegen.
func (s *Selection) FunctionName(init bool) string {
	if init {
		return fmt.Sprintf("i%d", s.ElementId())
	}
	return fmt.Sprintf("a%d", s.ElementId())
}

// Retrieves the merge function name used during codegen.
func (s *Selection) MergeFunctionName() string {
	return fmt.Sprintf("m%d", s.ElementId())
}

func (s *Selection) Fields() []*SelectionField {
	return s.fields
}

func (s *Selection) SetFields(fields []*SelectionField) {
	for _, f := range s.fields {
		f.SetParent(nil)
	}

	s.fields = fields

	for _, f := range s.fields {
		f.SetParent(s)
	}
}

// Determines if this selection contains non-aggregation fields.
func (s *Selection) HasNonAggregateFields() bool {
	for _, f := range s.fields {
		if !f.IsAggregate() {
			return true
		}
	}
	return false
}

//--------------------------------------
// Serialization
//--------------------------------------

// Encodes a query selection into an untyped map.
func (s *Selection) Serialize() map[string]interface{} {
	fields := []interface{}{}
	for _, field := range s.fields {
		fields = append(fields, field.Serialize())
	}

	obj := map[string]interface{}{
		"type":       TypeSelection,
		"name":       s.Name,
		"dimensions": s.Dimensions,
		"fields":     fields,
	}
	return obj
}

// Decodes a query selection from an untyped map.
func (s *Selection) Deserialize(obj map[string]interface{}) error {
	if obj == nil {
		return errors.New("Selection: Unable to deserialize nil.")
	}
	if obj["type"] != TypeSelection {
		return fmt.Errorf("Selection: Invalid statement type: %v", obj["type"])
	}

	// Deserialize "name".
	if name, ok := obj["name"].(string); ok {
		s.Name = name
	} else if obj["name"] == nil {
		s.Name = ""
	} else {
		return fmt.Errorf("Selection: Invalid name: %v", obj["name"])
	}

	// Deserialize "dimensions".
	if dimensions, ok := obj["dimensions"].([]interface{}); ok {
		s.Dimensions = []string{}
		for _, dimension := range dimensions {
			if str, ok := dimension.(string); ok {
				s.Dimensions = append(s.Dimensions, str)
			} else {
				return fmt.Errorf("Selection: Invalid dimension: %v", dimension)
			}
		}
	} else {
		if obj["dimension"] == nil {
			s.Dimensions = []string{}
		} else {
			return fmt.Errorf("Selection: Invalid dimensions: %v", obj["dimensions"])
		}
	}

	// Deserialize "fields".
	if arr, ok := obj["fields"].([]interface{}); ok {
		fields := []*SelectionField{}
		for _, field := range arr {
			if fieldMap, ok := field.(map[string]interface{}); ok {
				f := NewSelectionField("", "", nil)
				if err := f.Deserialize(fieldMap); err != nil {
					return err
				}
				fields = append(fields, f)
			} else {
				return fmt.Errorf("Selection: Invalid field: %v", field)
			}
		}
		s.SetFields(fields)
	} else if obj["field"] == nil {
		s.SetFields([]*SelectionField{})
	} else {
		return fmt.Errorf("Selection: Invalid fields: %v", obj["fields"])
	}

	return nil
}

//--------------------------------------
// Code Generation
//--------------------------------------

// Generates Lua code for the selection aggregation.
func (s *Selection) CodegenAggregateFunction(init bool) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate main function.
	fmt.Fprintf(buffer, "-- %s\n", s.String())
	fmt.Fprintf(buffer, "function %s(cursor, data)\n", s.FunctionName(init))

	// Add selection name.
	if s.Name != "" {
		fmt.Fprintf(buffer, "  if data[\"%s\"] == nil then data[\"%s\"] = {} end\n", s.Name, s.Name)
		fmt.Fprintf(buffer, "  data = data[\"%s\"]\n\n", s.Name)
	}

	// Group by dimension.
	for _, dimension := range s.Dimensions {
		name, root := dimension, "cursor.event"
		if name == "@eof" {
			name = "eof"
			root = "cursor.next_event"
		} else if name == "@eos" {
			name = "eos"
			root = "cursor.event"
		}
		fmt.Fprintf(buffer, "  dimension = %s:%s()\n", root, name)
		fmt.Fprintf(buffer, "  if data.%s == nil then data.%s = {} end\n", name, name)
		fmt.Fprintf(buffer, "  if data.%s[dimension] == nil then data.%s[dimension] = {} end\n", name, name)
		fmt.Fprintf(buffer, "  data = data.%s[dimension]\n\n", name)
	}

	// Create row object for non-aggregated selection.
	if s.HasNonAggregateFields() {
		fmt.Fprintf(buffer, "  row = {}\n\n")
	}

	// Select fields.
	for _, field := range s.fields {
		exp, err := field.CodegenExpression(init)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(buffer, "  "+exp)
	}

	// Append row object to "_".
	if s.HasNonAggregateFields() {
		fmt.Fprintf(buffer, "  if data._ == nil then data._ = {} end\n")
		fmt.Fprintf(buffer, "  table.insert(data._, row)\n")
	}

	// End function definition.
	fmt.Fprintln(buffer, "end")

	return buffer.String(), nil
}

// Generates Lua code for the selection merge.
func (s *Selection) CodegenMergeFunction(fields map[string]interface{}) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate nested functions first.
	code, err := s.CodegenInnerMergeFunction(0, s.Name, fields)
	if err != nil {
		return "", err
	}
	buffer.WriteString(code + "\n")

	// Generate main function.
	fmt.Fprintf(buffer, "-- %s\n", s.String())
	fmt.Fprintf(buffer, "function %s(result, data)\n", s.MergeFunctionName())
	if s.Name != "" {
		fmt.Fprintf(buffer, "  if result[\"%s\"] == nil then result[\"%s\"] = {} end\n", s.Name, s.Name)
		fmt.Fprintf(buffer, "  if data[\"%s\"] == nil then data[\"%s\"] = {} end\n", s.Name, s.Name)
		fmt.Fprintf(buffer, "  %sn0(result[\"%s\"], data[\"%s\"])\n", s.MergeFunctionName(), s.Name, s.Name)
	} else {
		fmt.Fprintf(buffer, "  %sn0(result, data)\n", s.MergeFunctionName())
	}
	fmt.Fprintf(buffer, "end\n")

	return buffer.String(), nil
}

// Generates Lua code for the inner merge.
func (s *Selection) CodegenInnerMergeFunction(index int, path string, fields map[string]interface{}) (string, error) {
	buffer := new(bytes.Buffer)

	// Generate next nested function first.
	if index < len(s.Dimensions) {
		path += "." + s.Dimensions[index]
		code, err := s.CodegenInnerMergeFunction(index+1, path, fields)
		if err != nil {
			return "", err
		}
		buffer.WriteString(code + "\n")
	}

	// Generate a rollup if our index points at a dimension. Otherwise generate
	// the leaf merge.
	fmt.Fprintf(buffer, "function %sn%d(result, data)\n", s.MergeFunctionName(), index)
	if index < len(s.Dimensions) {
		dimension := s.Dimensions[index]
		name := dimension
		if name[0] == '@' {
			name = name[1:]
		}
		fmt.Fprintf(buffer, "  if data ~= nil and data.%s ~= nil then\n", name)
		fmt.Fprintf(buffer, "    if result.%s == nil then result.%s = {} end\n", name, name)
		fmt.Fprintf(buffer, "    for k,v in pairs(data.%s) do\n", name)
		fmt.Fprintf(buffer, "      if result.%s[k] == nil then result.%s[k] = {} end\n", name, name)
		fmt.Fprintf(buffer, "      %sn%d(result.%s[k], v)\n", s.MergeFunctionName(), (index + 1), name)
		fmt.Fprintf(buffer, "    end\n")
		fmt.Fprintf(buffer, "  end\n")
	} else {
		// Merge fields.
		for _, field := range s.fields {
			fieldPath := path + "." + field.Name
			if fields[fieldPath] == nil {
				fields[fieldPath] = true

				exp, err := field.CodegenMergeExpression()
				if err != nil {
					return "", err
				}
				fmt.Fprintln(buffer, "  "+exp)
			}
		}

		// Merge non-aggregate selections.
		if s.HasNonAggregateFields() {
			fieldPath := path + "._"
			if fields[fieldPath] == nil {
				fields[fieldPath] = true
				fmt.Fprintf(buffer, "  if data ~= nil and data._ ~= nil then\n")
				fmt.Fprintf(buffer, "    if result._ == nil then result._ = {} end\n")
				fmt.Fprintf(buffer, "    for k,v in pairs(data._) do\n")
				fmt.Fprintf(buffer, "      table.insert(result._, v)\n")
				fmt.Fprintf(buffer, "    end\n")
				fmt.Fprintf(buffer, "  end\n")
			}
		}
	}
	fmt.Fprintf(buffer, "end\n")

	return buffer.String(), nil
}

//--------------------------------------
// Factorization
//--------------------------------------

// Converts factorized fields back to their original strings.
func (s *Selection) Defactorize(data interface{}) error {
	if m, ok := data.(map[interface{}]interface{}); ok {
		// If this is a named selection then drill in first.
		if s.Name != "" {
			if m2, ok := m[s.Name].(map[interface{}]interface{}); ok {
				m = m2
			} else {
				return nil
			}
		}

		if s.HasNonAggregateFields() && m["_"] != nil {
			if err := s.defactorizeNonAggregateFields(m["_"]); err != nil {
				return err
			}
		}

		// Recursively defactorize dimensions and then fields.
		return s.defactorize(m, 0)
	}

	return nil
}

// Recursively defactorizes dimensions.
func (s *Selection) defactorize(data interface{}, index int) error {
	query := s.Query()

	// Ignore any values that are nil or not maps.
	inner, ok := data.(map[interface{}]interface{})
	if !ok || data == nil {
		return nil
	}

	if index >= len(s.Dimensions) {
		if s.HasNonAggregateFields() && inner["_"] != nil {
			return s.defactorizeNonAggregateFields(inner["_"])
		}
		return nil
	}

	// Retrieve variable.
	dimension := s.Dimensions[index]
	variable := query.GetVariable(dimension)
	if variable == nil {
		return fmt.Errorf("Selection: Variable not found: %s", dimension)
	}

	// Defactorize.
	if outer, ok := inner[dimension].(map[interface{}]interface{}); ok {
		copy := map[interface{}]interface{}{}
		for k, v := range outer {
			if variable.DataType == db.Factor {
				// Only process this if it hasn't been defactorized already. Duplicate
				// defactorization can occur if there are multiple overlapping selections.
				if sequence, ok := normalize(k).(int64); ok {
					// If variable is associated with another variable then use that one
					// for factorization.
					factorDimension := dimension
					if variable.Association != "" {
						factorDimension = variable.Association
					}

					p, _ := query.Tx.Property(factorDimension)
					stringValue, err := query.Tx.Defactorize(p.ID, int(sequence))
					if err != nil {
						return err
					}
					copy[stringValue] = v
				} else {
					copy[k] = v
				}
			} else {
				copy[k] = v
			}

			// Defactorize next dimension.
			s.defactorize(v, index+1)
		}
		inner[dimension] = copy
	}

	return nil
}

// Defactorize non-aggregate field selection.
func (s *Selection) defactorizeNonAggregateFields(data interface{}) error {
	query := s.Query()

	// Ignore any values that are nil or not maps.
	inner, ok := data.(map[interface{}]interface{})
	if !ok || data == nil {
		return nil
	}

	// NOTE: This nesting below is crazy right now. This will probably be
	// refactored in the near future though.

	// Loop over each row and field.
	for _, row := range inner {
		if row, ok := row.(map[interface{}]interface{}); ok {
			for _, field := range s.fields {
				if ref, ok := field.Expression().(*VarRef); ok && !field.IsAggregate() {
					if variable, err := ref.Variable(); err != nil {
						return err
					} else if variable.DataType == db.Factor {
						name := field.CodegenName()
						if sequence, ok := normalize(row[name]).(int64); ok {
							factorName := variable.Name
							if variable.Association != "" {
								factorName = variable.Association
							}

							p, _ := query.Tx.Property(factorName)
							stringValue, err := query.Tx.Defactorize(p.ID, int(sequence))
							if err != nil {
								return err
							}
							row[name] = stringValue
						}
					}
				}
			}
		}
	}

	return nil
}

//--------------------------------------
// Finalization
//--------------------------------------

// Finalizes the results into a final state after merge.
func (s *Selection) Finalize(data interface{}) error {
	if m, ok := data.(map[interface{}]interface{}); ok {
		// If this is a named selection then drill in first.
		if s.Name != "" {
			if tmp, ok := m[s.Name].(map[interface{}]interface{}); ok {
				m = tmp
			} else {
				return nil
			}
		}

		if s.HasNonAggregateFields() && m["_"] != nil {
			m["_"] = s.finalizeNonAggregateFields(m["_"])
		}

		// Recursively defactorize dimensions and fields.
		return s.finalize(m, 0)
	}

	return nil
}

// Recursively finalizes dimensions.
func (s *Selection) finalize(data interface{}, index int) error {
	// Ignore any values that are nil or not maps.
	inner, ok := data.(map[interface{}]interface{})
	if !ok || data == nil {
		return nil
	}

	if s.HasNonAggregateFields() {
		inner["_"] = s.finalizeNonAggregateFields(inner["_"])
	}

	if index >= len(s.Dimensions) {
		for _, field := range s.fields {
			field.Finalize(inner)
		}
		return nil
	}

	// Defactorize.
	dimension := s.Dimensions[index]
	if outer, ok := inner[dimension].(map[interface{}]interface{}); ok {
		copy := map[interface{}]interface{}{}
		for k, v := range outer {
			s.finalize(v, index+1)
			copy[k] = v
		}
		inner[dimension] = copy
	}

	return nil
}

// Finalizes non-aggregate field selection.
func (s *Selection) finalizeNonAggregateFields(data interface{}) interface{} {
	if data, ok := data.(map[interface{}]interface{}); ok {
		ret := []interface{}{}
		for _, row := range data {
			if row, ok := row.(map[interface{}]interface{}); ok {
				obj := map[string]interface{}{}
				for k, v := range row {
					obj[k.(string)] = v
				}
				ret = append(ret, obj)
			}
		}
		return ret
	}
	return data
}

//--------------------------------------
// Initialization
//--------------------------------------

// Checks if any of the selection fields require initialization before
// performing aggregation.
func (s *Selection) RequiresInitialization() bool {
	for _, field := range s.fields {
		if field.RequiresInitialization() {
			return true
		}
	}
	return false
}

//--------------------------------------
// Utility
//--------------------------------------

// Returns a list of variable references used by this selection.
func (s *Selection) VarRefs() []*VarRef {
	refs := []*VarRef{}
	for _, dimension := range s.Dimensions {
		refs = append(refs, &VarRef{value: dimension})
	}
	for _, field := range s.fields {
		refs = append(refs, field.VarRefs()...)
	}
	return refs
}

// Returns a list of variables declared within this statement.
func (s *Selection) Variables() []*Variable {
	return []*Variable{}
}

// Converts the statements to a string-based representation.
func (s *Selection) String() string {
	str := "SELECT "

	arr := []string{}
	for _, field := range s.fields {
		arr = append(arr, field.String())
	}
	str += strings.Join(arr, ", ")

	if len(s.Dimensions) > 0 {
		str += " GROUP BY @" + strings.Join(s.Dimensions, ", @")
	}
	if s.Name != "" {
		str += " INTO " + strconv.Quote(s.Name)
	}
	return str
}
