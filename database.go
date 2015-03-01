package garbage5

import (
	"encoding/binary"
	"github.com/karlseguin/bolt"
	"gopkg.in/karlseguin/bytepool.v3"
	"sync"
)

var (
	SETS      = []byte("sets")
	LISTS     = []byte("lists")
	RESOURCES = []byte("resources")
	bp        = bytepool.NewEndian(65536, 64, binary.LittleEndian)
)

type Resource interface {
	Id() string
	Bytes() []byte
}

type Database struct {
	storage *bolt.DB

	ids      *IdMap
	setLock  sync.RWMutex
	listLock sync.RWMutex
	sets     map[string]Set
	lists    map[string]List
	results  *ResultPool
}

func New(c *Configuration) (*Database, error) {
	db, err := bolt.Open(c.path, 0600, nil)
	if err != nil {
		return nil, err
	}
	database := &Database{
		storage: db,
		ids:     NewIdMap(),
		sets:    make(map[string]Set),
		lists:   make(map[string]List),
	}
	database.results = NewResultPool(c.maxResults, 128)
	if err := database.initialize(); err != nil {
		db.Close()
		return nil, err
	}
	return database, nil
}

func (db *Database) initialize() error {
	err := db.storage.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(SETS); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(LISTS); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(RESOURCES); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	db.loadIds(SETS, func(name string, ids []uint32) {
		db.sets[name] = NewSet(ids)
	})
	db.loadIds(LISTS, func(name string, ids []uint32) {
		db.lists[name] = NewList(ids)
	})
	return nil
}

// Returns the list. The list is unlocked; consumers are responsible for locking
// and unlocking the list (Lock/RLock/Unlock/RUnlock). Changes to the list will
// not be persisted.
func (db *Database) List(name string) List {
	defer db.listLock.RUnlock()
	db.listLock.RLock()
	return db.lists[name]
}

// Returns the set. The set is unlocked; consumers are responsible for locking
// and unlocking the set (Lock/RLock/Unlock/RUnlock). Changes to the set will
// not be persisted.
func (db *Database) Set(name string) Set {
	defer db.setLock.RUnlock()
	db.setLock.RLock()
	return db.sets[name]
}

// Creates, or overwirtes, an in-memory and on-disk list
func (db *Database) CreateList(name string, ids ...string) error {
	if err := db.writeIds(LISTS, name, ids); err != nil {
		return err
	}
	list := NewList(db.toInts(ids))
	db.listLock.Lock()
	db.lists[name] = list
	db.listLock.Unlock()
	return nil
}

// Creates, or overwirtes, an in-memory and on-disk set
func (db *Database) CreateSet(name string, ids ...string) error {
	if err := db.writeIds(SETS, name, ids); err != nil {
		return err
	}
	set := NewSet(db.toInts(ids))
	db.setLock.Lock()
	db.sets[name] = set
	db.setLock.Unlock()
	return nil
}

// Store a resource
func (db *Database) Put(resource Resource) error {
	return db.storage.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(RESOURCES).Put([]byte(resource.Id()), resource.Bytes())
	})
}

func (db *Database) Query(sort string) *Query {
	return NewQuery(sort, db)
}

// Close the database
func (db *Database) Close() error {
	return db.storage.Close()
}

func (db *Database) toInts(values []string) []uint32 {
	l := len(values)
	ids := make([]uint32, l)
	for i := 0; i < l; i++ {
		ids[i] = db.ids.Internal(values[i], true)
	}
	return ids
}

func (db *Database) writeIds(bucket []byte, name string, ids []string) error {
	l := len(ids)
	buffer := bp.Checkout()
	defer buffer.Release()
	buffer.WriteUint32(uint32(l))
	for i := 0; i < l; i++ {
		id := ids[i]
		buffer.WriteByte(byte(len(id)))
		buffer.Write([]byte(id))
	}

	return db.storage.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Put([]byte(name), buffer.Bytes())
	})
}

func (db *Database) loadIds(bucket []byte, fn func(name string, ids []uint32)) {
	db.storage.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			l := binary.LittleEndian.Uint32(v)
			ids := make([]uint32, l)
			position := 4
			for i := 0 * l; i < l; i++ {
				start := position + 1
				end := start + int(v[position])
				ids[i] = db.ids.Internal(string(v[start:end]), true)
				position = end
			}
			fn(string(k), ids)
		}
		return nil
	})
}
