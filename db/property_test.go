package db_test

import (
	"errors"
	"testing"

	. "github.com/skydb/sky/db"
	"github.com/stretchr/testify/assert"
)

// Ensure that property names cannot be blank.
func TestPropertyValidateNameNonBlank(t *testing.T) {
	p := &Property{DataType: "string"}
	assert.Equal(t, p.Validate(), errors.New("invalid property name: "))
}

// Ensure that property names cannot have invalid characters.
func TestPropertyValidateNameInvalidCharacters(t *testing.T) {
	p := &Property{Name: `foo\bar`, DataType: "string"}
	assert.Equal(t, p.Validate(), errors.New("invalid property name: foo\\bar"))
}

// Ensure that property has a valid data type.
func TestPropertyValidateDataType(t *testing.T) {
	p := &Property{Name: `foo`, DataType: "ahhh!!"}
	assert.Equal(t, p.Validate(), errors.New("invalid data type: ahhh!!"))
}

// Ensure that a String property can cast a value appropriately.
func TestPropertyStringCast(t *testing.T) {
	p := &Property{DataType: String}
	assert.Equal(t, p.Cast("foo"), "foo")
	assert.Equal(t, p.Cast(0), "")
}

// Ensure that a Factor property can cast a value appropriately.
func TestPropertyFactorCast(t *testing.T) {
	p := &Property{DataType: String}
	assert.Equal(t, p.Cast("foo"), "foo")
	assert.Equal(t, p.Cast(0), "")
}

// Ensure that an Integer property can cast a value appropriately.
func TestPropertyIntegerCast(t *testing.T) {
	p := &Property{DataType: Integer}
	assert.Equal(t, p.Cast(100), int64(100))
	assert.Equal(t, p.Cast(float64(20.4)), int64(20))
	assert.Equal(t, p.Cast("foo"), int64(0))
}

// Ensure that an Float property can cast a value appropriately.
func TestPropertyFloatCast(t *testing.T) {
	p := &Property{DataType: Float}
	assert.Equal(t, p.Cast(float64(100.3)), float64(100.3))
	assert.Equal(t, p.Cast(100), float64(100))
	assert.Equal(t, p.Cast("foo"), float64(0))
}

// Ensure that an Boolean property can cast a value appropriately.
func TestPropertyBooleanCast(t *testing.T) {
	p := &Property{DataType: Boolean}
	assert.Equal(t, p.Cast(true), true)
	assert.Equal(t, p.Cast(false), false)
	assert.Equal(t, p.Cast(100), false)
	assert.Equal(t, p.Cast("true"), false)
}

// Ensure that an invalid property performs no casting.
func TestPropertyInvalidCast(t *testing.T) {
	p := &Property{DataType: "bad-type"}
	assert.Equal(t, p.Cast(true), true)
	assert.Equal(t, p.Cast(100), 100)
	assert.Equal(t, p.Cast("foo"), "foo")
}
