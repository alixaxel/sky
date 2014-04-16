package db

import (
	"encoding/binary"
	"fmt"
	"github.com/skydb/sky/core"
	"github.com/boltdb/bolt"
	"os"
	"sync"
	"strconv"
)

// cacheSize is the number of factors that are stored in the LRU cache.
// This cache size is per-property.
const cacheSize = 1000

// name of the factor db bucket
var bucket = []byte("factors")

// Factorizer manages the factorization and defactorization of values for a table.
type Factorizer struct {
	sync.Mutex

	db 		*bolt.DB
	txn		*bolt.Tx
	path   	string
	caches 	map[string]*cache
	dirty  	bool
}

// NewFactorizer returns a new Factorizer instance.
func NewFactorizer() *Factorizer {
	return &Factorizer{}
}

// Path is the location of the factors database on disk.
func (f *Factorizer) Path() string {
	return f.path
}

// Open bolt database at the given path.
func (f *Factorizer) Open(path string) error {
	var err error
	
	f.Lock()
	defer f.Unlock()

	// Close the factorizer if it's already open.
	f.close()

	// Initialize and open the database.
	f.path = path
	if err = os.MkdirAll(f.path, 0700); err != nil {
		return err
	}

	f.db, err = bolt.Open(f.path, 0664)
	if err != nil {
		f.close()
		return fmt.Errorf("factor database open error: %s", err)
	}

	f.renew()
	
	if err := f.txn.CreateBucketIfNotExists(bucket); err != nil {
		return fmt.Errorf("factor bucket creation error: %s", err)
	}
	
	// Initialize the cache.
	f.caches = make(map[string]*cache)

	return nil
}

// Close releases all factor resources.
func (f *Factorizer) Close() {
	f.Lock()
	defer f.Unlock()
	f.close()
}

func (f *Factorizer) close() {
	f.path = ""
	f.caches = nil
	if f.txn != nil {
		f.txn.Commit()
		f.txn = nil
	}
	if f.db != nil {
		f.db.Close()
		f.db = nil
	}
}

// Factorize converts a value for a property into a numeric identifier.
// If a value has already been factorized then it is reused. Otherwise a new one is created.
func (f *Factorizer) Factorize(id string, value string, createIfMissing bool) (uint64, error) {
	f.Lock()
	defer f.Unlock()
	defer f.renew()
	return f.factorize(id, value, createIfMissing)
}

// Defactorize converts a previously factorized value from its numeric identifier
// to its string representation.
func (f *Factorizer) Defactorize(id string, value uint64) (string, error) {
	f.Lock()
	defer f.Unlock()
	defer f.renew()
	return f.defactorize(id, value)
}

// FactorizeEvent converts all the values of an event into their numeric identifiers.
func (f *Factorizer) FactorizeEvent(event *core.Event, propertyFile *core.PropertyFile, createIfMissing bool) error {
	if event == nil {
		return nil
	}

	f.Lock()
	defer f.Unlock()
	defer f.renew()

	for k, v := range event.Data {
		property := propertyFile.GetProperty(k)
		if property.DataType == core.FactorDataType {
			if stringValue, ok := v.(string); ok {
				sequence, err := f.factorize(property.Name, stringValue, createIfMissing)
				if err != nil {
					return err
				}
				event.Data[k] = sequence
			}
		}
	}

	return nil
}

// DefactorizeEvent converts all the values of an event from their numeric identifiers to their string values.
func (f *Factorizer) DefactorizeEvent(event *core.Event, propertyFile *core.PropertyFile) error {
	if event == nil {
		return nil
	}

	f.Lock()
	defer f.Unlock()
	defer f.renew()

	for k, v := range event.Data {
		property := propertyFile.GetProperty(k)
		if property.DataType == core.FactorDataType {
			var sequence uint64
			switch v := v.(type) {
			case int8:
				sequence = uint64(v)
			case int16:
				sequence = uint64(v)
			case int32:
				sequence = uint64(v)
			case int64:
				sequence = uint64(v)
			case uint8:
				sequence = uint64(v)
			case uint16:
				sequence = uint64(v)
			case uint32:
				sequence = uint64(v)
			case uint64:
				sequence = v
			case float32:
				sequence = uint64(v)
			case float64:
				sequence = uint64(v)
			}
			stringValue, err := f.defactorize(property.Name, uint64(sequence))
			if err != nil {
				return err
			}
			event.Data[k] = stringValue
		}
	}

	return nil
}
func (f *Factorizer) factorize(id string, value string, createIfMissing bool) (uint64, error) {
	// Blank is always zero.
	if value == "" {
		return 0, nil
	}

	// Check the LRU first.
	c := f.cache(id)
	if sequence, ok := c.getValue(value); ok {
		return sequence, nil
	}

	bucket := f.txn.Bucket(bucket)
	
	data := bucket.Get(f.key(value))
	if data != nil {
		sequence := binary.BigEndian.Uint64(data)
		c.add(value, sequence)
		return sequence, nil
	}

	// Create a new factor if requested.
	if createIfMissing {
		return f.add(bucket, id, value)
	}

	return 0, NewFactorNotFound(fmt.Sprintf("factor not found: %s: %v", id, f.key(value)))
}

// add creates a new factor for a given value.
func (f *Factorizer) add(bucket *bolt.Bucket, id string, value string) (uint64, error) {
	// Retrieve next id in sequence.
	seq, err := bucket.NextSequence()
	if err != nil {
		return 0, err
	}
	sequence := uint64(seq)

	// Store the value-to-id lookup.
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], sequence)
	if err := bucket.Put(f.key(value), data[:]); err != nil {
		return 0, err
	}

	// Save the id-to-value lookup.
	if err := bucket.Put(f.revkey(sequence), []byte(value)); err != nil {
		return 0, err
	}

	// Add to cache.
	c := f.cache(id)
	c.add(value, sequence)

	return sequence, nil
}

func (f *Factorizer) defactorize(id string, value uint64) (string, error) {
	// Blank is always zero.
	if value == 0 {
		return "", nil
	}

	// Check the cache first.
	c := f.cache(id)
	if key, ok := c.getKey(value); ok {
		return key, nil
	}

	bucket := f.txn.Bucket(bucket)
	
	data := bucket.Get(f.revkey(value))

	if data == nil {
		return "", fmt.Errorf("factor not found: %v", f.revkey(value))
	}

	// Add to cache.
	c.add(string(data), value)

	return string(data), nil
}

// renew commits any dirty changes on the transaction and renews it.
func (f *Factorizer) renew() error {
	// Commit if dirty.
	if f.dirty {
		f.dirty = false
		if err := f.txn.Commit(); err != nil {
			return err
		}
		f.txn = nil
	}

	// Create a new transaction if needed.
	if f.txn == nil {
		var err error
		if f.txn, err = f.db.Begin(true); err != nil {
			return fmt.Errorf("renew txn error: %s", err)
		}
	}

	return nil
}

// cache returns a reference to the LRU cache used for a given tablespace/id.
// If a cache doesn't exist then one will be created.
func (f *Factorizer) cache(id string) *cache {
	c := f.caches[id]
	if c == nil {
		c = newCache(cacheSize)
		f.caches[id] = c
	}
	return c
}

// The key for a given value.
func (f *Factorizer) key(value string) []byte {
	b := make([]byte,len(value)+1)
	b[0] = '>'
	copy(b[1:],value)
	return b
}

// The reverse key for a given value.
func (f *Factorizer) revkey(value uint64) []byte {
	b := make([]byte,17)
	b[0] = '<'
	b = strconv.AppendUint(b[:1],value,16)
	return b 
}
