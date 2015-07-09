package indexes

import (
	"encoding/binary"
	"sync"
)

var (
	Endianness     = binary.LittleEndian
	DefaultPayload = []byte("null")
	IdSize         = 4
)

type Id uint32

type Storage interface {
	Close() error
	ListCount() uint32
	SetCount() uint32
	Fill(miss []interface{}, payloads [][]byte) error
	Get(id Id) ([]byte, bool)
	LoadNResources(n int) (map[Id][]byte, error)
	LoadIds(newOnly bool) (map[string]Id, error)
	EachSet(newOnly bool, f func(name string, ids []Id)) error
	EachList(newOnly bool, f func(name string, ids []Id)) error
	ClearNew() error
	UpsertSet(id string, payload []byte) ([]Id, error)
	UpsertList(id string, payload []byte) ([]Id, error)
	UpsertResource(id Id, eid string, summary []byte, details []byte) error
	RemoveSet(id string) error
	RemoveList(id string) error
	RemoveResource(id Id) error
	UpdateIds(blob []byte, estimatedCount int) (map[string]Id, error)
}

type Resource interface {
	Id() string
	Bytes() []byte
}

type Database struct {
	queries  QueryPool
	cache    *Cache
	idLock   sync.RWMutex
	setLock  sync.RWMutex
	listLock sync.RWMutex
	storage  Storage
	ids      map[string]Id
	sets     map[string]Set
	lists    map[string]List
}

func New(c *Configuration) (*Database, error) {
	database := &Database{}
	storage, err := database.initialize(c)
	if err != nil {
		if storage != nil {
			storage.Close()
		}
		return nil, err
	}
	database.storage = storage

	cache, err := newCache(storage, c)
	if err != nil {
		database.Close()
		return nil, err
	}
	database.cache = cache
	database.queries = NewQueryPool(database, c.maxSets, c.maxResults)
	return database, nil
}

func (db *Database) initialize(c *Configuration) (storage Storage, err error) {
	storage, err = newSqliteStorage(c.path)
	if err != nil {
		return nil, err
	}
	db.sets = make(map[string]Set, storage.SetCount())
	db.lists = make(map[string]List, storage.ListCount())
	return storage, db.loadData(false, storage)
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

func (db *Database) Get(id string) []byte {
	db.idLock.RLock()
	iid, exists := db.ids[id]
	db.idLock.RUnlock()
	if exists == false {
		return nil
	}
	return db.cache.Fetch(iid)
}

func (db *Database) Query() *Query {
	return db.queries.Checkout()
}

func (db *Database) Reload() error {
	return db.loadData(true, db.storage)
}

func (db *Database) UpdateSet(name string, blob []byte) error {
	ids, err := db.storage.UpsertSet(name, blob)
	if err != nil {
		return err
	}
	set := NewSet(ids)
	db.setLock.Lock()
	db.sets[name] = set
	db.setLock.Unlock()
	return nil
}

func (db *Database) RemoveSet(name string) error {
	if err := db.storage.RemoveSet(name); err != nil {
		return err
	}
	db.setLock.Lock()
	delete(db.sets, name)
	db.setLock.Unlock()
	return nil
}

func (db *Database) UpdateList(name string, blob []byte) error {
	ids, err := db.storage.UpsertList(name, blob)
	if err != nil {
		return err
	}
	list := NewList(ids)
	db.listLock.Lock()
	db.lists[name] = list
	db.listLock.Unlock()
	return nil
}

func (db *Database) RemoveList(name string) error {
	if err := db.storage.RemoveList(name); err != nil {
		return err
	}
	db.listLock.Lock()
	delete(db.lists, name)
	db.listLock.Unlock()
	return nil
}

func (db *Database) UpdateResource(id Id, eid string, summary []byte, details []byte) error {
	if err := db.storage.UpsertResource(id, eid, summary, details); err != nil {
		return err
	}
	db.cache.Set(id, summary, false)
	db.cache.Set(id, details, true)
	return nil
}

func (db *Database) RemoveResource(id Id) error {
	if err := db.storage.RemoveResource(id); err != nil {
		return err
	}
	db.cache.Remove(id)
	return nil
}

func (db *Database) UpdateIds(blob []byte) error {
	db.idLock.RLock()
	currentSize := len(db.ids)
	db.idLock.RUnlock()

	ids, err := db.storage.UpdateIds(blob, currentSize)
	if err != nil {
		return err
	}

	db.idLock.Lock()
	db.ids = ids
	db.idLock.Unlock()
	return nil
}

// Close the database
func (db *Database) Close() error {
	return db.storage.Close()
}

func (db *Database) loadData(newOnly bool, storage Storage) error {
	ids, err := storage.LoadIds(newOnly)
	if err != nil {
		return err
	}
	db.idLock.Lock()
	db.ids = ids
	db.idLock.Unlock()

	err = storage.EachSet(newOnly, func(name string, ids []Id) {
		set := NewSet(ids)
		db.setLock.Lock()
		db.sets[name] = set
		db.setLock.Unlock()
	})
	if err != nil {
		return err
	}

	err = storage.EachList(newOnly, func(name string, ids []Id) {
		list := NewList(ids)
		db.listLock.Lock()
		db.lists[name] = list
		db.listLock.Unlock()

		db.setLock.Lock()
		db.sets[name] = list
		db.setLock.Unlock()
	})
	return err
}
