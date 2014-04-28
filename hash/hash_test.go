package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Ensure that the local hash is consistent.
func TestLocal(t *testing.T) {
	assert.Equal(t, Local("foobar"), uint32(0xf73967e8))
}

// Ensure that the remote hash is consistent.
func TestRemote(t *testing.T) {
	assert.Equal(t, Remote("foobar"), uint32(0x85944171))
}
