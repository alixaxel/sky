package db

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
	"path"

	"github.com/skydb/sky/core"
	"github.com/boltdb/bolt"
	"github.com/ugorji/go/codec"
)

// shard represents a subset of the database stored in a single LMDB environment.
type shard struct {
	sync.Mutex
	path string
	db  *bolt.DB
}

// newShard creates a new shard.
func newShard(path string) *shard {
	return &shard{path: path}
}

func (s *shard) Stats() *bolt.Stats {
	stats := s.db.Stats()
	return &stats
}

// Open allocates a new LMDB environment.
func (s *shard) Open() error {
	s.Lock()
	defer s.Unlock()
	s.close()

	if err := os.MkdirAll(path.Dir(s.path), 0700); err != nil {
		return err
	}

	var err error
	s.db, err = bolt.Open(s.path, 0664)
	if err != nil {
		return fmt.Errorf("shard database open error: %s", err)
	}

	return nil
}

// Close releases all shard resources.
func (s *shard) Close() {
	s.Lock()
	defer s.Unlock()
	s.close()
}

func (s *shard) close() {
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}
}

// Cursor retrieves a cursor for iterating over the shard.
func (s *shard) Cursor(tablespace string) (*bolt.Cursor, error) {
	s.Lock()
	defer s.Unlock()

	txn, table, err := s.table(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %s (%s)", err, tablespace)
	}

	c := table.Cursor()
	if c == nil {
		txn.Rollback()
		return nil, fmt.Errorf("failed to create cursor (%s)", tablespace)
	}

	return c, err
}


// InsertEvent adds a single event to the shard.
func (s *shard) InsertEvent(tablespace string, id string, event *core.Event) error {
	s.Lock()
	defer s.Unlock()

	txn, table, err := s.table(tablespace, false)
	if err != nil {
		return fmt.Errorf("failed to open table: %s (%s)", err, tablespace)
	}
	defer txn.Rollback()

	if err := s.insertEvent(table, id, core.ShiftTimeBytes(event.Timestamp), event.Data); err != nil {
		return err
	}
	txn.Commit()

	return nil
}

func (s *shard) insertEvent(table *bolt.Bucket, id string, timestamp []byte, data map[int64]interface{}) error {
	// Get event at timestamp and merge if existing.
	if old, err := s.getEvent(table, id, timestamp); err != nil {
		return err
	} else if old != nil {
		for k, v := range data {
			old[k] = v
		}
		data = old
	}
    // Encode timestamp.
    var b bytes.Buffer
    if _, err := b.Write(timestamp); err != nil {
	    return err
    }

	// Encode data.
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewEncoder(&b, &handle).Encode(data); err != nil {
		return err
	}

	// Insert event.
	object, err := table.CreateBucketIfNotExists([]byte(id))
	if err != nil {
		return fmt.Errorf("error creating object: %s (id=%s)", err, id)	
	}

	if err := object.Put(timestamp, b.Bytes()); err != nil {
		return fmt.Errorf("error writing event: %s (%s: len=%d)", err, id, b.Len())
	}

	return nil
}

// InsertEvents adds a multiple events for an object to the shard.
func (s *shard) InsertEvents(tablespace string, id string, events []*core.Event) error {
	s.Lock()
	defer s.Unlock()

	txn, table, err := s.table(tablespace, false)
	if err != nil {
		return fmt.Errorf("failed to open table: %s", err)
	}
	defer txn.Rollback()

	for _, event := range events {
		if err := s.insertEvent(table, id, core.ShiftTimeBytes(event.Timestamp), event.Data); err != nil {
			return err
		}
	}
	txn.Commit()

	return nil
}

// Retrieves an event for a given object at a single point in time.
func (s *shard) GetEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error) {
	s.Lock()
	defer s.Unlock()

	txn, table, err := s.table(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %s (%s)", err, tablespace)
	}
	defer txn.Rollback()

	data, err := s.getEvent(table, id, core.ShiftTimeBytes(timestamp))
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	return &core.Event{Timestamp: timestamp, Data: data}, nil
}

func (s *shard) getEvent(table *bolt.Bucket, id string, timestamp []byte) (map[int64]interface{}, error) {
	var object *bolt.Bucket
	var err error
	if table.Writable() {
		object, err = table.CreateBucketIfNotExists([]byte(id))
		if err != nil {
			return nil, fmt.Errorf("failed to create object: %s (%s)", err, id)		
		}
	} else {
		if object = table.Bucket([]byte(id)); object == nil {
			return nil, nil
		}
	}

	event := object.Get(timestamp)
	if event == nil {
		return nil, nil	
	}

	if !bytes.Equal(timestamp, event[0:8]) {
		return nil,nil 
	}
	
	// Decode data.
	var data = make(map[int64]interface{})
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewDecoder(bytes.NewBuffer(event[8:]), &handle).Decode(&data); err != nil {
		return nil, err
	}
	for k, v := range data {
		data[k] = normalize(v)
	}

	return data, nil
}

// Retrieves a list of events for a given object in a table.
func (s *shard) GetEvents(tablespace string, id string) ([]*core.Event, error) {
	s.Lock()
	defer s.Unlock()

	var events = make([]*core.Event, 0)

	txn, table, err := s.table(tablespace, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %s (%s)", err, tablespace)
	}
	defer txn.Rollback()

	object := table.Bucket([]byte(id))
	if object == nil {
		return nil, nil		
	}

	c := object.Cursor()
	if c == nil {
		return nil, fmt.Errorf("failed to open cursor: %s (%s)", id, tablespace)
	}

	for key, val := c.First(); key != nil; key, val = c.Next() {

		// Create event.
		event := &core.Event{
			Timestamp: core.UnshiftTimeBytes(key),
			Data:      make(map[int64]interface{}),
		}

		// Decode data.
		var handle codec.MsgpackHandle
		handle.RawToString = true
		if err := codec.NewDecoder(bytes.NewBuffer(val[8:]), &handle).Decode(&event.Data); err != nil {
			return nil, err
		}
		for k, v := range event.Data {
			event.Data[k] = normalize(v)
		}

		events = append(events, event)
	}

	return events, nil
}

// DeleteEvent removes a single event from the shard.
func (s *shard) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s.Lock()
	defer s.Unlock()

	txn, table, err := s.table(tablespace, false)
	if err != nil {
		return fmt.Errorf("failed to open table: %s (%s)", err, tablespace)
	}
	defer txn.Rollback()

	object := table.Bucket([]byte(id))
	if object == nil {
		return fmt.Errorf("object not found: %s (%s)", id, tablespace)		
	}

	if err = object.Delete(core.ShiftTimeBytes(timestamp)); err != nil {
		return err
	}
	txn.Commit()

	return nil
}

// Deletes all events for a given object in a table.
func (s *shard) DeleteObject(tablespace, id string) error {
	s.Lock()
	defer s.Unlock()

	// Begin a transaction.
	txn, table, err := s.table(tablespace, false)
	if err != nil {
		return fmt.Errorf("failed to open table: %s (%s)", err, tablespace)
	}
	defer txn.Rollback()

	// Delete the key.
	if err = table.DeleteBucket([]byte(id)); err != nil {
		return fmt.Errorf("object delete error: %s (%s)", err, id)
	}
	txn.Commit()

	return nil
}

// Drop removes a table from the shard.
func (s *shard) Drop(tablespace string) error {
	s.Lock()
	defer s.Unlock()
	return s.drop(tablespace)
}

func (s *shard) drop(tablespace string) error {
	txn, err := s.db.Begin(true)
	if err != nil {
		return fmt.Errorf("Unable to start bolt transaction: %s", err)
	}
	defer txn.Rollback()

	// Delete the key.
	if err = txn.DeleteBucket([]byte(tablespace)); err != nil {
		return fmt.Errorf("table delete error: %s (%s)", err, tablespace)
	}
	txn.Commit()
	
	return nil
}

func (s *shard) table(tablespace string, readOnly bool) (*bolt.Tx, *bolt.Bucket, error) {
	txn, err := s.db.Begin(!readOnly)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to start bolt transaction: %s", err)
	}
	var bucket *bolt.Bucket
	if readOnly {
		bucket = txn.Bucket([]byte(tablespace))
		if bucket == nil {
			txn.Rollback()
			return nil, nil, fmt.Errorf("Table does not exist: %s", tablespace)
		}		
	} else {
		bucket, err = txn.CreateBucketIfNotExists([]byte(tablespace))
		if err != nil {
			txn.Rollback()
			return nil, nil, fmt.Errorf("Failed to create table: %s (%s)", err, tablespace)
		}		
	}
	return txn, bucket, nil
}
