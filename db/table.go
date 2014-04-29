package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/skydb/sky/hash"
	"github.com/ugorji/go/codec"
)

// FactorCacheSize is the number of factors that are stored in the LRU cache.
// This cache size is per-property.
const FactorCacheSize = 1000

var (
	// ErrObjectIDRequired is returned inserting, deleting, or retrieving
	// event data without specifying an object identifier.
	ErrObjectIDRequired = errors.New("object id required")
)

// NewTable returns a reference to a new table.
func NewTable(name, path string) *Table {
	return &Table{
		name: name,
		path: path,
	}
}

// Table represents a collection of objects.
type Table struct {
	sync.Mutex

	db             *bolt.DB
	name           string
	path           string
	caches         map[int]*cache
	properties     map[string]*Property
	propertiesByID map[int]*Property
	stat           Stat

	shardCount     int
	maxPermanentID int
	maxTransientID int
}

// Name returns the name of the table.
func (t *Table) Name() string {
	return t.name
}

// Path returns the location of the table on disk.
func (t *Table) Path() string {
	return t.path
}

// ShardCount returns the number of shards in the table.
func (t *Table) ShardCount() int {
	return t.shardCount
}

// Exists returns whether the table exists.
func (t *Table) Exists() bool {
	_, err := os.Stat(t.path)
	return !os.IsNotExist(err)
}

func (t *Table) Create() error {
	t.Lock()
	defer t.Unlock()

	// Set initial shard count.
	t.shardCount = 16
	if t.shardCount == 0 {
		t.shardCount = runtime.NumCPU()
	}

	// Open the table.
	if err := t.open(); err != nil {
		return err
	}

	// Save initial table state.
	err := t.Update(func(tx *Tx) error {
		return tx.PutMeta()
	})
	if err != nil {
		return err
	}

	return nil
}

// Open opens and initializes the table.
func (t *Table) Open() error {
	t.Lock()
	defer t.Unlock()
	return t.open()
}

func (t *Table) open() error {
	if t.db != nil {
		return nil
	}

	// Create Bolt database.
	db, err := bolt.Open(t.path, 0600)
	if err != nil {
		return fmt.Errorf("table open: %s", err)
	}
	t.db = db

	// Initialize schema.
	err = t.Update(func(tx *Tx) error {
		// Create meta bucket.
		b, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return fmt.Errorf("meta: %s", err)
		}

		// Read meta data into table.
		value := b.Get([]byte("meta"))
		if len(value) > 0 {
			if err := t.unmarshal(value); err != nil {
				return err
			}
		}

		// Create shard buckets.
		for i := 0; i < t.shardCount; i++ {
			if _, err := tx.CreateBucketIfNotExists(shardDBName(i)); err != nil {
				return fmt.Errorf("shard: %s", err)
			}
		}

		// Create factor buckets.
		for _, p := range t.properties {
			if p.DataType != Factor {
				continue
			}
			if _, err := tx.CreateBucketIfNotExists(factorDBName(p.ID)); err != nil {
				return fmt.Errorf("factor: %s", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Initialize the factor caches.
	t.caches = make(map[int]*cache)
	for _, p := range t.properties {
		if p.DataType == Factor {
			t.caches[p.ID] = newCache(FactorCacheSize)
		}
	}

	return nil
}

// drop closes and removes the table.
func (t *Table) drop() error {
	t.Lock()
	defer t.Unlock()

	// Close table and delete everything.
	t.close()
	if err := os.RemoveAll(t.path); err != nil {
		return fmt.Errorf("remove all error: %s", err)
	}

	return nil
}

// opened returned whether the table is currently open.
func (t *Table) opened() bool {
	return t.db != nil
}

func (t *Table) Close() {
	t.Lock()
	defer t.Unlock()
	t.close()
}

func (t *Table) close() {
	if t.db != nil {
		t.db.Close()
	}
}

// View executes a function in the context of a read-only transaction.
func (t *Table) View(fn func(*Tx) error) error {
	return t.db.View(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, t})
	})
}

// Update executes a function in the context of a writable transaction.
func (t *Table) Update(fn func(*Tx) error) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx, t})
	})
}

// MaxTransientID returns the largest transient property identifier.
func (t *Table) MaxTransientID() int {
	return t.maxTransientID
}

// MaxPermanentID returns the largest transient property identifier.
func (t *Table) MaxPermanentID() int {
	return t.maxPermanentID
}

// marshal encodes the table into a byte slice.
func (t *Table) marshal() ([]byte, error) {
	var msg = tableRawMessage{Name: t.name, ShardCount: t.shardCount, MaxPermanentID: t.maxPermanentID, MaxTransientID: t.maxTransientID}
	for _, p := range t.properties {
		msg.Properties = append(msg.Properties, p)
	}
	return json.Marshal(msg)
}

// unmarshal decodes a byte slice into a table.
func (t *Table) unmarshal(data []byte) error {
	var msg tableRawMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	t.name = msg.Name
	t.maxPermanentID = msg.MaxPermanentID
	t.maxTransientID = msg.MaxTransientID
	t.shardCount = msg.ShardCount

	t.properties = make(map[string]*Property)
	t.propertiesByID = make(map[int]*Property)
	for _, p := range msg.Properties {
		p.table = t
		t.properties[p.Name] = p
		t.propertiesByID[p.ID] = p
	}

	return nil
}

// copyProperties creates a new map and copies all existing properties.
func (t *Table) copyProperties() {
	properties := make(map[string]*Property)
	for k, v := range t.properties {
		properties[k] = v
	}
	t.properties = properties

	propertiesByID := make(map[int]*Property)
	for k, v := range t.propertiesByID {
		propertiesByID[k] = v
	}
	t.propertiesByID = propertiesByID
}

// shardIndex returns the appropriate shard for a given object id.
func (t *Table) shardIndex(id string) int {
	return int(hash.Local(id)) % t.shardCount
}

// Tx represents a transaction.
type Tx struct {
	*bolt.Tx
	Table *Table
}

// PutMeta persists the meta data for the table.
func (tx *Tx) PutMeta() error {
	value, err := tx.Table.marshal()
	if err != nil {
		return fmt.Errorf("marshal: %s", err)
	}
	return tx.Bucket([]byte("meta")).Put([]byte("meta"), value)
}

// Shards returns a slice of shard buckets.
func (tx *Tx) Shards() []*bolt.Bucket {
	var buckets = make([]*bolt.Bucket, 0)
	for i := 0; i < tx.Table.shardCount; i++ {
		buckets = append(buckets, tx.Bucket(shardDBName(i)))
	}
	return buckets
}

// Properties retrieves a map of properties by property name.
func (tx *Tx) Properties() (map[string]*Property, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}
	return tx.Table.properties, nil
}

// Properties retrieves a map of properties by property identifier.
func (tx *Tx) PropertiesByID() (map[int]*Property, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}
	return tx.Table.propertiesByID, nil
}

// Property returns a single property from the table with the given name.
func (tx *Tx) Property(name string) (*Property, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}
	return tx.Table.properties[name], nil
}

// PropertyByID returns a single property from the table by id.
func (tx *Tx) PropertyByID(id int) (*Property, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}
	return tx.Table.propertiesByID[id], nil
}

// CreateProperty creates a new property on the table.
func (tx *Tx) CreateProperty(name string, dataType string, transient bool) (*Property, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}

	// Don't allow duplicate names.
	if tx.Table.properties[name] != nil {
		return nil, fmt.Errorf("property already exists: %s", name)
	}

	// Create and validate property.
	p := &Property{
		table:     tx.Table,
		Name:      name,
		Transient: transient,
		DataType:  dataType,
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}

	// Retrieve the next property id.
	if transient {
		tx.Table.maxTransientID--
		p.ID = tx.Table.maxTransientID
	} else {
		tx.Table.maxPermanentID++
		p.ID = tx.Table.maxPermanentID
	}

	// Initialize factor database.
	if p.DataType == Factor {
		if _, err := tx.CreateBucketIfNotExists(factorDBName(p.ID)); err != nil {
			return nil, fmt.Errorf("factor: %s", err)
		}
	}

	// Add it to the collection.
	properties, propertiesByID := tx.Table.properties, tx.Table.propertiesByID
	tx.Table.copyProperties()
	tx.Table.properties[name] = p
	tx.Table.propertiesByID[p.ID] = p

	if err := tx.PutMeta(); err != nil {
		tx.Table.properties = properties
		tx.Table.propertiesByID = propertiesByID
		return nil, err
	}

	// Initialize the cache.
	if p.DataType == Factor {
		tx.Table.caches[p.ID] = newCache(FactorCacheSize)
	}

	return p, nil
}

// RenameProperty updates the name of a property.
func (tx *Tx) RenameProperty(oldName, newName string) (*Property, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	} else if tx.Table.properties[oldName] == nil {
		return nil, fmt.Errorf("property not found: %s", oldName)
	} else if tx.Table.properties[newName] != nil {
		return nil, fmt.Errorf("property already exists: %s", newName)
	}

	properties := tx.Table.properties
	tx.Table.copyProperties()
	p := tx.Table.properties[oldName].Clone()
	p.Name = newName
	delete(tx.Table.properties, oldName)
	tx.Table.properties[newName] = p

	if err := tx.PutMeta(); err != nil {
		tx.Table.properties = properties
		return nil, err
	}
	return p, nil
}

// DeleteProperty removes a single property from the table.
func (tx *Tx) DeleteProperty(name string) error {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return fmt.Errorf("table not open: %s", tx.Table.name)
	}

	p := tx.Table.properties[name]
	if p == nil {
		return fmt.Errorf("property not found: %s", name)
	}

	properties, propertiesByID := tx.Table.properties, tx.Table.propertiesByID
	tx.Table.copyProperties()
	delete(tx.Table.properties, name)
	delete(tx.Table.propertiesByID, p.ID)

	if err := tx.PutMeta(); err != nil {
		tx.Table.properties = properties
		tx.Table.propertiesByID = propertiesByID
		return err
	}
	return nil
}

// Event returns a single event for an object at a given timestamp.
func (tx *Tx) Event(id string, timestamp time.Time) (*Event, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}

	rawEvent, err := tx.getRawEvent(id, shiftTime(timestamp))
	if err != nil {
		return nil, err
	} else if rawEvent == nil {
		return nil, nil
	}

	return tx.toEvent(rawEvent)
}

// Events returns all events for an object in chronological order.
func (tx *Tx) Events(id string) ([]*Event, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return nil, fmt.Errorf("table not open: %s", tx.Table.name)
	}

	// Retrieve raw events.
	rawEvents, err := tx.getRawEvents(id)
	if err != nil {
		return nil, err
	}

	// Convert to regular events and return.
	var events []*Event
	for _, rawEvent := range rawEvents {
		event, err := tx.toEvent(rawEvent)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (tx *Tx) getRawEvent(id string, timestamp int64) (*rawEvent, error) {
	if id == "" {
		return nil, ErrObjectIDRequired
	}

	var stat = bench()

	// Find object bucket.
	b := tx.Bucket(shardDBName(tx.Table.shardIndex(id))).Bucket([]byte(id))
	if b == nil {
		return nil, nil
	}

	// Find event.
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(timestamp))

	value := b.Get(key[:])
	if value == nil {
		return nil, nil
	}
	stat.count++
	stat.apply(&tx.Table.stat.Event.Fetch.Count, &tx.Table.stat.Event.Fetch.Duration)

	// Unmarshal bytes into a raw event.
	stat = bench()
	e := &rawEvent{}
	if err := e.unmarshal(value); err != nil {
		return nil, err
	}
	stat.count++
	stat.apply(&tx.Table.stat.Event.Unmarshal.Count, &tx.Table.stat.Event.Unmarshal.Duration)

	return e, nil
}

func (tx *Tx) getRawEvents(id string) ([]*rawEvent, error) {
	if id == "" {
		return nil, ErrObjectIDRequired
	}

	// Find object's bucket.
	b := tx.Bucket(shardDBName(tx.Table.shardIndex(id))).Bucket([]byte(id))
	if b == nil {
		return nil, nil
	}

	// Loop over each event.
	var stat = bench()
	var slices [][]byte
	_ = b.ForEach(func(k, v []byte) error {
		slices = append(slices, v)
		return nil
	})
	stat.count += len(slices)
	stat.apply(&tx.Table.stat.Event.Fetch.Count, &tx.Table.stat.Event.Fetch.Duration)

	// Unmarshal each slice into a raw event.
	stat = bench()
	var events []*rawEvent
	for _, b := range slices {
		e := &rawEvent{}
		if err := e.unmarshal(b); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	stat.count += len(events)
	stat.apply(&tx.Table.stat.Event.Unmarshal.Count, &tx.Table.stat.Event.Unmarshal.Duration)
	return events, nil
}

// InsertEvent inserts an event for an object.
func (tx *Tx) InsertEvent(id string, event *Event) error {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return fmt.Errorf("table not open: %s", tx.Table.name)
	}
	return tx.insertEvent(id, event)
}

// InsertEvents inserts multiple events for an object.
func (tx *Tx) InsertEvents(id string, events []*Event) error {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return fmt.Errorf("table not open: %s", tx.Table.name)
	}
	for _, event := range events {
		if err := tx.insertEvent(id, event); err != nil {
			return err
		}
	}
	return nil
}

// InsertObjects inserts multiple sets of events for different objects.
func (tx *Tx) InsertObjects(objects map[string][]*Event) error {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return fmt.Errorf("table not open: %s", tx.Table.name)
	}
	for id, events := range objects {
		for _, event := range events {
			if err := tx.insertEvent(id, event); err != nil {
				return fmt.Errorf("insert objects error: %s", err)
			}
		}
	}
	return nil
}

func (tx *Tx) insertEvent(id string, e *Event) error {
	if id == "" {
		return ErrObjectIDRequired
	}

	// Convert to raw event.
	rawEvent, err := tx.toRawEvent(e)
	if err != nil {
		return fmt.Errorf("insert event error: %s", err)
	}

	// Insert event into appropriate shard.
	b, err := tx.Bucket(shardDBName(tx.Table.shardIndex(id))).CreateBucketIfNotExists([]byte(id))
	if err != nil {
		return err
	}

	// Marshal raw event into byte slice.
	var stat = bench()
	value, err := rawEvent.marshal()
	if err != nil {
		return err
	}
	stat.count++
	stat.apply(&tx.Table.stat.Event.Marshal.Count, &tx.Table.stat.Event.Marshal.Duration)

	// Insert key/value.
	stat = bench()
	var key = make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(rawEvent.timestamp))

	if err := b.Put(key, value); err != nil {
		return err
	}

	stat.count++
	stat.apply(&tx.Table.stat.Event.Insert.Count, &tx.Table.stat.Event.Insert.Duration)
	return nil
}

// DeleteEvent removes a single event for an object at a given timestamp.
func (tx *Tx) DeleteEvent(id string, timestamp time.Time) error {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return fmt.Errorf("table not open: %s", tx.Table.name)
	}

	// Find object bucket.
	b := tx.Bucket(shardDBName(tx.Table.shardIndex(id))).Bucket([]byte(id))
	if b == nil {
		return nil
	}

	// Create the timestamp prefix.
	var stat = bench()
	var key = make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(shiftTime(timestamp)))

	// Delete the event.
	if err := b.Delete(key); err != nil {
		return err
	}

	stat.count++
	stat.apply(&tx.Table.stat.Event.Delete.Count, &tx.Table.stat.Event.Delete.Duration)
	return nil
}

// DeleteEvents removes all events for an object.
func (tx *Tx) DeleteEvents(id string) error {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	if !tx.Table.opened() {
		return fmt.Errorf("table not open: %s", tx.Table.name)
	}

	// Delete object.
	return tx.Bucket(shardDBName(tx.Table.shardIndex(id))).DeleteBucket([]byte(id))
}

// Merge combines two existing objects together.
func (tx *Tx) Merge(destId, srcId string) error {
	panic("not implemented: Tx.Merge()")
	return nil
}

// Keys returns all object keys in the table in sorted order.
func (tx *Tx) Keys() []string {
	var keys = make([]string, 0)
	for i := 0; i < tx.Table.shardCount; i++ {
		_ = tx.Bucket(shardDBName(i)).ForEach(func(k, _ []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	}
	sort.Strings(keys)
	return keys
}

// Factorize converts a factor property value to its integer index representation.
func (tx *Tx) Factorize(propertyID int, value string) (int, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	return tx.factorize(propertyID, value, false)
}

// factorize converts a factor property value to its integer index representation.
// Returns an error if the factor could not be found and createIfNotExists is false.
func (tx *Tx) factorize(propertyID int, value string, createIfNotExists bool) (int, error) {
	// Blank is always zero.
	if value == "" {
		return 0, nil
	}

	// Check the LRU first.
	if sequence, ok := tx.Table.caches[propertyID].getValue(value); ok {
		tx.Table.stat.Event.Factorize.CacheHit.Count++
		return sequence, nil
	}

	// Find an existing factor for the value.
	var stat = bench()
	stat.count++
	var val int
	if data := tx.Bucket(factorDBName(propertyID)).Get(factorKey(value)); data != nil {
		val = int(binary.BigEndian.Uint64(data))
	}
	if val != 0 {
		stat.apply(&tx.Table.stat.Event.Factorize.FetchHit.Count, &tx.Table.stat.Event.Factorize.FetchHit.Duration)
		tx.Table.caches[propertyID].add(value, val)
		return val, nil
	}
	stat.apply(&tx.Table.stat.Event.Factorize.FetchMiss.Count, &tx.Table.stat.Event.Factorize.FetchMiss.Duration)

	// Create a new factor if requested.
	if createIfNotExists {
		return tx.addFactor(propertyID, value)
	}

	return 0, nil
}

func (tx *Tx) addFactor(propertyID int, value string) (int, error) {
	var index int
	var stat = bench()

	// Look up next sequence index.
	var b = tx.Bucket(factorDBName(propertyID))
	index, err := b.NextSequence()
	if err != nil {
		return 0, err
	}

	// Store the value-to-index lookup.
	var data = make([]byte, 8)
	binary.BigEndian.PutUint64(data[:], uint64(index))
	if err := b.Put(factorKey(value), data); err != nil {
		return 0, fmt.Errorf("add factor txn put error: %s", err)
	}

	// Save the index-to-value lookup.
	if err := b.Put(reverseFactorKey(index), []byte(value)); err != nil {
		return 0, fmt.Errorf("add factor put reverse error: %s", err)
	}

	// Add to cache.
	tx.Table.caches[propertyID].add(value, index)

	stat.count++
	stat.apply(&tx.Table.stat.Event.Factorize.Create.Count, &tx.Table.stat.Event.Factorize.Create.Duration)

	return index, nil
}

// Defactorize converts a factor index to its actual value.
func (tx *Tx) Defactorize(propertyID int, index int) (string, error) {
	tx.Table.Lock()
	defer tx.Table.Unlock()
	return tx.defactorize(propertyID, index)
}

// defactorize converts a factor index to its string value.
func (tx *Tx) defactorize(propertyID int, index int) (string, error) {
	// Blank is always zero.
	if index == 0 {
		return "", nil
	}

	// Check the cache first.
	if key, ok := tx.Table.caches[propertyID].getKey(index); ok {
		tx.Table.stat.Event.Defactorize.CacheHit.Count++
		return key, nil
	}

	var stat = bench()
	stat.count++
	var data = tx.Bucket(factorDBName(propertyID)).Get(reverseFactorKey(index))
	if data == nil {
		stat.apply(&tx.Table.stat.Event.Defactorize.FetchMiss.Count, &tx.Table.stat.Event.Defactorize.FetchMiss.Duration)
		return "", fmt.Errorf("factor not found: %d: %d", propertyID, index)
	}
	stat.apply(&tx.Table.stat.Event.Defactorize.FetchHit.Count, &tx.Table.stat.Event.Defactorize.FetchHit.Duration)

	// Add to cache.
	tx.Table.caches[propertyID].add(string(data), index)

	return string(data), nil
}

// shardDBName returns the name of the shard table.
func shardDBName(index int) []byte {
	return []byte(fmt.Sprintf("shards/%d", index))
}

// factorDBName returns the name of the factor table for a property.
func factorDBName(propertyID int) []byte {
	return []byte(fmt.Sprintf("factors/%d", propertyID))
}

// factorKey returns the value-to-index key.
func factorKey(value string) []byte {
	return []byte(fmt.Sprintf(">%s", value))
}

// reverseFactorKey returns the index-to-value key.
func reverseFactorKey(index int) []byte {
	return []byte(fmt.Sprintf("<%d", index))
}

// toRawEvent returns a raw event representation of this event.
func (tx *Tx) toRawEvent(e *Event) (*rawEvent, error) {
	rawEvent := &rawEvent{
		timestamp: shiftTime(e.Timestamp),
		data:      make(map[int]interface{}),
	}

	// Map data by property id instead of name.
	for k, v := range e.Data {
		p := tx.Table.properties[k]
		if p == nil {
			return nil, fmt.Errorf("property not found: %s", k)
		}

		// Cast the value to the appropriate type.
		v = p.Cast(v)

		// Factorize value, if needed.
		if p.DataType == Factor {
			var err error
			v, err = tx.factorize(p.ID, v.(string), true)
			if err != nil {
				return nil, err
			}
		}

		rawEvent.data[p.ID] = v
	}

	return rawEvent, nil
}

// toEvent returns an normal event representation of this raw event.
func (tx *Tx) toEvent(e *rawEvent) (*Event, error) {
	event := &Event{
		Timestamp: unshiftTime(e.timestamp),
		Data:      make(map[string]interface{}),
	}

	// Map data by name instead of property id.
	for k, v := range e.data {
		p := tx.Table.propertiesByID[k]

		// Missing properties have been deleted so just ignore.
		if p == nil {
			continue
		}

		// Cast the value to the appropriate type.
		v = promote(v)

		// Defactorize value, if needed.
		if p.DataType == Factor {
			var err error
			if intValue, ok := v.(int64); ok {
				v, err = tx.defactorize(p.ID, int(intValue))
				if err != nil {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("invalid factor value: %v", v)
			}
		}

		event.Data[p.Name] = v
	}

	return event, nil
}

type tableRawMessage struct {
	Name           string      `json:"name"`
	ShardCount     int         `json:"shardCount"`
	MaxPermanentID int         `json:"maxPermanentID"`
	MaxTransientID int         `json:"maxTransientID"`
	Properties     []*Property `json:"properties"`
}

// Event represents the state for an object at a given point in time.
type Event struct {
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

// rawEvent represents an internal event structure.
type rawEvent struct {
	timestamp int64
	data      map[int]interface{}
}

// marshal encodes the raw event as a byte slice.
func (e *rawEvent) marshal() ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, e.timestamp)
	assert(err == nil, "timestamp marshal error: %v", err)

	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewEncoder(&buf, &handle).Encode(e.data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// unmarshal decodes a raw event from a byte slice.
func (e *rawEvent) unmarshal(b []byte) error {
	var buf = bytes.NewBuffer(b)
	err := binary.Read(buf, binary.BigEndian, &e.timestamp)
	assert(err == nil, "timestamp unmarshal error: %v", err)

	e.data = make(map[int]interface{})
	var handle codec.MsgpackHandle
	handle.RawToString = true
	if err := codec.NewDecoder(buf, &handle).Decode(&e.data); err != nil {
		return err
	}
	e.normalize()

	return nil
}

// normalize promotes all values of the raw event to appropriate types.
func (e *rawEvent) normalize() {
	for k, v := range e.data {
		e.data[k] = promote(v)
	}
}

// stat represents a simple counter and timer.
type stat struct {
	count int
	time  time.Time
}

// since returns the elapsed time since the stat began.
func (s *stat) since() time.Duration {
	return time.Since(s.time)
}

// apply increments the count and duration based on the stat.
func (s *stat) apply(count *int, duration *time.Duration) {
	*count += s.count
	*duration += time.Since(s.time)
}

// bench begins a timed stat counter.
func bench() stat {
	return stat{0, time.Now()}
}

// Stat represents statistics for a single table.
type Stat struct {
	Event struct {
		Fetch struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"fetch"`
		Insert struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"insert"`
		Delete struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"delete"`
		Factorize struct {
			CacheHit struct {
				Count int `json:"count"`
			} `json:"cacheHit"`
			FetchHit struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchHit"`
			FetchMiss struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchMiss"`
			Create struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"create"`
		} `json:"factorize"`
		Defactorize struct {
			CacheHit struct {
				Count int `json:"count"`
			} `json:"cacheHit"`
			FetchHit struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchHit"`
			FetchMiss struct {
				Count    int           `json:"count"`
				Duration time.Duration `json:"duration"`
			} `json:"fetchMiss"`
		} `json:"defactorize"`
		Marshal struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"marshal"`
		Unmarshal struct {
			Count    int           `json:"count"`
			Duration time.Duration `json:"duration"`
		} `json:"unmarshal"`
	} `json:"event"`
}

// Diff calculates the difference between a stat object and another.
func (s *Stat) Diff(other *Stat) *Stat {
	diff := &Stat{}
	diff.Event.Fetch.Count = s.Event.Fetch.Count - other.Event.Fetch.Count
	diff.Event.Fetch.Duration = s.Event.Fetch.Duration - other.Event.Fetch.Duration
	diff.Event.Insert.Count = s.Event.Insert.Count - other.Event.Insert.Count
	diff.Event.Insert.Duration = s.Event.Insert.Duration - other.Event.Insert.Duration
	diff.Event.Delete.Count = s.Event.Delete.Count - other.Event.Delete.Count
	diff.Event.Delete.Duration = s.Event.Delete.Duration - other.Event.Delete.Duration
	diff.Event.Factorize.CacheHit.Count = s.Event.Factorize.CacheHit.Count - other.Event.Factorize.CacheHit.Count
	diff.Event.Factorize.FetchHit.Count = s.Event.Factorize.FetchHit.Count - other.Event.Factorize.FetchHit.Count
	diff.Event.Factorize.FetchHit.Duration = s.Event.Factorize.FetchHit.Duration - other.Event.Factorize.FetchHit.Duration
	diff.Event.Factorize.FetchMiss.Count = s.Event.Factorize.FetchMiss.Count - other.Event.Factorize.FetchMiss.Count
	diff.Event.Factorize.FetchMiss.Duration = s.Event.Factorize.FetchMiss.Duration - other.Event.Factorize.FetchMiss.Duration
	diff.Event.Factorize.Create.Count = s.Event.Factorize.Create.Count - other.Event.Factorize.Create.Count
	diff.Event.Factorize.Create.Duration = s.Event.Factorize.Create.Duration - other.Event.Factorize.Create.Duration
	diff.Event.Defactorize.CacheHit.Count = s.Event.Defactorize.CacheHit.Count - other.Event.Defactorize.CacheHit.Count
	diff.Event.Defactorize.FetchHit.Count = s.Event.Defactorize.FetchHit.Count - other.Event.Defactorize.FetchHit.Count
	diff.Event.Defactorize.FetchHit.Duration = s.Event.Defactorize.FetchHit.Duration - other.Event.Defactorize.FetchHit.Duration
	diff.Event.Defactorize.FetchMiss.Count = s.Event.Defactorize.FetchMiss.Count - other.Event.Defactorize.FetchMiss.Count
	diff.Event.Defactorize.FetchMiss.Duration = s.Event.Defactorize.FetchMiss.Duration - other.Event.Defactorize.FetchMiss.Duration
	diff.Event.Marshal.Count = s.Event.Marshal.Count - other.Event.Marshal.Count
	diff.Event.Marshal.Duration = s.Event.Marshal.Duration - other.Event.Marshal.Duration
	diff.Event.Unmarshal.Count = s.Event.Unmarshal.Count - other.Event.Unmarshal.Count
	diff.Event.Unmarshal.Duration = s.Event.Unmarshal.Duration - other.Event.Unmarshal.Duration
	return diff
}
