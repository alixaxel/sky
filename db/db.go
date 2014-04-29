package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// DefaultMaxDBs is the default limit of databases Sky can use.
	DefaultMaxDBs = uint(1024)

	// DefaultMaxReaders is the default limit of readers that Sky can use.
	DefaultMaxReaders = uint(126)
)

var (
	// ErrDatabaseNotOpen is returned when an operation is being attempted but
	// the database is not in an open state.
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrTableNameRequired is returned when open a table without a name.
	ErrTableNameRequired = errors.New("table name required")
)

// DB represents Sky's file-backed data store.
type DB struct {
	sync.RWMutex
	NoSync     bool
	MaxDBs     uint
	MaxReaders uint

	tables map[string]*Table
	path   string
}

// Path returns the root path of the database.
func (db *DB) Path() string {
	return db.path
}

// tablePath returns the path of a named table.
func (db *DB) tablePath(name string) string {
	return filepath.Join(db.path, name)
}

// Open opens and initializes the database.
func (db *DB) Open(path string) error {
	db.Lock()
	defer db.Unlock()

	// Return an error if the database is currently opened.
	if db.path != "" {
		return fmt.Errorf("database already open")
	}

	// Initialize poperties.
	db.path = path
	db.tables = make(map[string]*Table)

	// Create directory if it doesn't exist.
	if err := os.MkdirAll(db.path, 0700); err != nil {
		return fmt.Errorf("db open error: %s", err)
	}

	return nil
}

// Close closes all tables.
func (db *DB) Close() {
	db.Lock()
	defer db.Unlock()
	db.close()
}

func (db *DB) close() {
	for _, t := range db.tables {
		t.close()
	}
	db.tables = nil
	db.path = ""
}

// CreateTable creates, opens and initializes a table.
// Returns an error if the table already exists.
func (db *DB) CreateTable(name string, shardCount int) (*Table, error) {
	db.Lock()
	defer db.Unlock()
	if db.path == "" {
		return nil, ErrDatabaseNotOpen
	} else if name == "" {
		return nil, ErrTableNameRequired
	}

	// Make sure the table doesn't exist.
	t := db.table(name)
	if t.Exists() {
		return nil, fmt.Errorf("table already exists: %s", name)
	}

	// Create table.
	t.shardCount = shardCount
	if err := t.Create(); err != nil {
		return nil, err
	}
	db.tables[name] = t

	// Return opened table.
	return t, nil
}

// DropTable closes and removes a table from the database.
// Returns an error if the database does not exist.
func (db *DB) DropTable(name string) error {
	db.Lock()
	defer db.Unlock()
	if db.path == "" {
		return ErrDatabaseNotOpen
	}

	// Find the table.
	t := db.table(name)
	if !t.Exists() {
		return fmt.Errorf("table not found: %s", name)
	}

	// Drop table and remove it from the cache.
	if err := t.drop(); err != nil {
		return err
	}
	delete(db.tables, name)

	return nil
}

// OpenTable opens and initializes a table.
// Returns an error if the table does not exist.
func (db *DB) OpenTable(name string) (*Table, error) {
	db.Lock()
	defer db.Unlock()

	if db.path == "" {
		return nil, ErrDatabaseNotOpen
	} else if name == "" {
		return nil, ErrTableNameRequired
	}

	t := db.table(name)
	if !t.Exists() {
		return nil, fmt.Errorf("table not found: %s", name)
	}

	if err := t.Open(); err != nil {
		return nil, err
	}

	// Cache the table and return it.
	db.tables[name] = t
	return t, nil
}

// table creates a reference to a table associated with this database.
// Returns a reference to an existing table if one already exists.
func (db *DB) table(name string) *Table {
	// Check for an already open table.
	if t, ok := db.tables[name]; ok {
		return t
	}

	// Create table instance and return it.
	return NewTable(name, db.tablePath(name))
}
