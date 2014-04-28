package db

import (
	"testing"

	_assert "github.com/stretchr/testify/assert"
)

func TestPromote(t *testing.T) {
	_assert.Equal(t, promote(int(10)), int64(10))
	_assert.Equal(t, promote(int8(10)), int64(10))
	_assert.Equal(t, promote(int16(10)), int64(10))
	_assert.Equal(t, promote(int32(10)), int64(10))
	_assert.Equal(t, promote(int64(10)), int64(10))
	_assert.Equal(t, promote(uint(10)), int64(10))
	_assert.Equal(t, promote(uint8(10)), int64(10))
	_assert.Equal(t, promote(uint16(10)), int64(10))
	_assert.Equal(t, promote(uint32(10)), int64(10))
	_assert.Equal(t, promote(uint64(10)), int64(10))

	_assert.Equal(t, promote(float32(100.0)), float64(100.0))
	_assert.Equal(t, promote(float64(100.0)), float64(100.0))
}
