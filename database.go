package indexes

import (
	"encoding/binary"
	"sync"

	"gopkg.in/karlseguin/bytepool.v3"
)

var (
	Endianness = binary.LittleEndian
	bp         = bytepool.NewEndian(65536, 64, Endianness)
)

type Id uint32

type Storage interface {
	Close() error
	ListCount() uint32
	SetCount() uint32
	Fetch(miss []*Miss) error
	EachSet(func(name string, ids []Id)) error
	EachList(func(name string, ids []Id)) error
}

type Resource interface {
	Id() string
	Bytes() []byte
}

type Database struct {
	path      string
	queries   QueryPool
	resources *Resources
	setLock   sync.RWMutex
	listLock  sync.RWMutex
	storage   Storage
	sets      map[string]Set
	lists     map[string]List
}

func New(c *Configuration) (*Database, error) {
	database := &Database{
		path: c.path,
	}
	storage, err := database.initialize()
	if err != nil {
		if storage != nil {
			storage.Close()
		}
		return nil, err
	}

	database.storage = storage
	database.resources = newResources(storage.Fetch, c)
	database.queries = NewQueryPool(database, c.maxSets, c.maxResults)

	return database, nil
}

func (db *Database) initialize() (Storage, error) {
	storage, err := newSqliteStorage(db.path)
	if err != nil {
		return nil, err
	}

	db.sets = make(map[string]Set, storage.SetCount())
	db.lists = make(map[string]List, storage.ListCount())

	if err != nil {
		return storage, err
	}

	err = storage.EachSet(func(name string, ids []Id) {
		db.sets[name] = NewSet(ids)
	})
	if err != nil {
		return storage, err
	}

	err = storage.EachList(func(name string, ids []Id) {
		list := NewList(ids)
		db.lists[name] = list
		db.sets[name] = list
	})
	return storage, err
}

// Returns the list. The list is unlocked; consumers are responsible for locking
// and unlocking the list (Lock/RLock/Unlock/RUnlock). Changes to the list will
// not be persisted.
func (db *Database) GetList(name string) List {
	defer db.listLock.RUnlock()
	db.listLock.RLock()
	return db.lists[name]
}

// Returns the set. The set is unlocked; consumers are responsible for locking
// and unlocking the set (Lock/RLock/Unlock/RUnlock). Changes to the set will
// not be persisted.
func (db *Database) GetSet(name string) Set {
	db.setLock.RLock()
	s, exists := db.sets[name]
	db.setLock.RUnlock()
	if exists == false {
		return EmptySet
	}
	return s
}

func (db *Database) Query() *Query {
	return db.queries.Checkout()
}

// Close the database
func (db *Database) Close() error {
	return db.storage.Close()
}
