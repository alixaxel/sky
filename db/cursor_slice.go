package db

import "github.com/boltdb/bolt"

// Cursors is a list of read cursors.
type Cursors []*bolt.Cursor

// Close deallocates all cursor resources.
func (s Cursors) Close() {
	for _, c := range s {
		c.Tx().Rollback()
	}
}
