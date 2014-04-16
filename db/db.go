package db

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/skydb/sky/core"
	"github.com/boltdb/bolt"
)

var defaultShardCount = runtime.NumCPU()

// func init() { defaultShardCount = 1 }

// DB represents access to the low-level data store.
type DB interface {
	Open() error
	Close()
	Factorizer(tablespace string) (*Factorizer, error)
	Cursors(tablespace string) Cursors
	GetEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error)
	GetEvents(tablespace string, id string) ([]*core.Event, error)
	InsertEvent(tablespace string, id string, event *core.Event) error
	InsertEvents(tablespace string, id string, newEvents []*core.Event) error
	InsertObjects(tablespace string, objects map[string][]*core.Event) (int, error)
	DeleteEvent(tablespace string, id string, timestamp time.Time) error
	DeleteObject(tablespace string, id string) error
	Merge(tablespace string, destinationId string, sourceId string) error
	Drop(tablespace string) error
	Stats() []*bolt.Stats
}

// db is the default implementation of the DB interface.
type db struct {
	sync.RWMutex

	factorizers map[string]*Factorizer
	path        string
	shards      []*shard
}

// Creates a new DB instance with data storage at the given path.
func New(path string) DB {
	return &db{
		factorizers: make(map[string]*Factorizer),
		path:        path,
	}
}

func (db *db) dataPath() string {
	return filepath.Join(db.path, "data")
}

func (db *db) factorsPath() string {
	return filepath.Join(db.path, "factors")
}

func (db *db) shardPath(index int) string {
	return filepath.Join(db.dataPath(), strconv.Itoa(index))
}

// Opens the database.
func (db *db) Open() error {
	db.Lock()
	defer db.Unlock()

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(db.dataPath(), 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(db.factorsPath(), 0700); err != nil {
		return err
	}

	// Determine shard count.
	shardCount, err := db.shardCount()
	if err != nil {
		return err
	}

	// Create and open each shard.
	db.shards = make([]*shard, 0)
	for i := 0; i < shardCount; i++ {
		db.shards = append(db.shards, newShard(db.shardPath(i)))
		if err := db.shards[i].Open(); err != nil {
			db.close()
			return err
		}
	}

	return nil
}

// Close shuts down all open database resources.
func (db *db) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
}

func (db *db) close() {
	for _, f := range db.factorizers {
		f.Close()
	}
	db.factorizers = nil

	for _, s := range db.shards {
		s.Close()
	}
	db.shards = nil
}

// getShardByObjectId retrieves the appropriate shard for a given object identifier.
func (db *db) getShardByObjectId(id string) *shard {
	index := core.ObjectLocalHash(id) % uint32(len(db.shards))
	return db.shards[index]
}

// shardCount retrieves the number of shards in the database. This is determined
// by the number of numeric directories in the data path. If no directories exist
// then a default count is used.
func (db *db) shardCount() (int, error) {
	infos, err := ioutil.ReadDir(db.dataPath())
	if err != nil {
		return 0, err
	}

	count := 0
	for _, info := range infos {
		index, err := strconv.Atoi(info.Name())
		if err == nil && (index+1) > count {
			count = index + 1
		}
	}

	if count == 0 {
		count = defaultShardCount
	}

	return count, nil
}

// LockAll obtains locks on the database and all shards.
func (db *db) LockAll() {
	db.Lock()
	for _, s := range db.shards {
		s.Lock()
	}
}

// UnlockAll releases locks on the database and all shards.
// Only use this with LockAll().
func (db *db) UnlockAll() {
	db.Unlock()
	for _, s := range db.shards {
		s.Unlock()
	}
}

// Factorizer returns a table's factorizer.
func (db *db) Factorizer(tablespace string) (*Factorizer, error) {
	db.Lock()
	defer db.Unlock()
	return db.factorizer(tablespace)
}

func (db *db) factorizer(tablespace string) (*Factorizer, error) {
	// Retrieve already open factorizer if available.
	if f := db.factorizers[tablespace]; f != nil {
		return f, nil
	}

	// Otherwise create a new factorizer for the table.
	f := NewFactorizer()

	path := filepath.Join(db.factorsPath(), tablespace)
	if err := f.Open(path); err != nil {
		return nil, err
	}

	// Save the open factorizer to the lookup.
	db.factorizers[tablespace] = f

	return f, nil
}

// Cursors retrieves a set of cursors for iterating over the database.
func (db *db) Cursors(tablespace string) Cursors {
	cursors := make(Cursors, 0)
	for _, s := range db.shards {
		c, err := s.Cursor(tablespace)
		if err == nil {
			cursors = append(cursors, c)
		}
	}
	return cursors
}

func (db *db) GetEvent(tablespace string, id string, timestamp time.Time) (*core.Event, error) {
	s := db.getShardByObjectId(id)
	return s.GetEvent(tablespace, id, timestamp)
}

func (db *db) GetEvents(tablespace string, id string) ([]*core.Event, error) {
	s := db.getShardByObjectId(id)
	return s.GetEvents(tablespace, id)
}

// InsertEvent adds a single event to the database.
func (db *db) InsertEvent(tablespace string, id string, event *core.Event) error {
	s := db.getShardByObjectId(id)
	return s.InsertEvent(tablespace, id, event)
}

// InsertEvents adds multiple events for a single object.
func (db *db) InsertEvents(tablespace string, id string, newEvents []*core.Event) error {
	s := db.getShardByObjectId(id)
	return s.InsertEvents(tablespace, id, newEvents)
}

// InsertObjects bulk inserts events for multiple objects.
func (db *db) InsertObjects(tablespace string, objects map[string][]*core.Event) (int, error) {
	count := 0
	for id, events := range objects {
		s := db.getShardByObjectId(id)
		if err := s.InsertEvents(tablespace, id, events); err != nil {
			return count, err
		}
		count += len(events)
	}
	return count, nil
}

func (db *db) DeleteEvent(tablespace string, id string, timestamp time.Time) error {
	s := db.getShardByObjectId(id)
	return s.DeleteEvent(tablespace, id, timestamp)
}

func (db *db) DeleteObject(tablespace string, id string) error {
	s := db.getShardByObjectId(id)
	return s.DeleteObject(tablespace, id)
}

func (db *db) Merge(tablespace string, destinationId string, sourceId string) error {
	dest := db.getShardByObjectId(destinationId)
	src := db.getShardByObjectId(sourceId)

	// Retrieve source events.
	srcEvents, err := src.GetEvents(tablespace, sourceId)
	if err != nil {
		return err
	}

	// Insert events into destination object.
	if len(srcEvents) > 0 {
		if err = dest.InsertEvents(tablespace, destinationId, srcEvents); err != nil {
			return err
		}
		if err = src.DeleteObject(tablespace, sourceId); err != nil {
			return err
		}
	}

	return nil
}

// Drop removes a table from the database.
func (db *db) Drop(tablespace string) error {
	var err error
	for _, s := range db.shards {
		if _err := s.Drop(tablespace); err == nil {
			err = _err
		}
	}
	return err
}

func (db *db) Stats() []*bolt.Stats {
	stats := make([]*bolt.Stats, 0, len(db.shards))
	for _, shard := range db.shards {
		stats = append(stats, shard.Stats())
	}
	return stats
}
