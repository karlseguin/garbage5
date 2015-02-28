package garbage5

import (
	"github.com/karlseguin/bolt"
	"gopkg.in/karlseguin/bytepool.v3"
	"gopkg.in/karlseguin/idmap.v1"
	"sync"
)

var (
	SETS      = []byte("sets")
	LISTS     = []byte("lists")
	RESOURCES = []byte("resources")
	bp        = bytepool.New(65536, 64)
)

type Database struct {
	storage *bolt.DB

	ids      *idmap.Map32
	setLock  sync.RWMutex
	listLock sync.RWMutex
	sets     map[string]Set
	lists    map[string]List
}

func New(path string) (*Database, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	database := &Database{
		storage: db,
		ids:     idmap.New32(16),
		sets:    make(map[string]Set),
		lists:   make(map[string]List),
	}
	if err := database.initialize(); err != nil {
		db.Close()
		return nil, err
	}
	return database, nil
}

func (db *Database) initialize() error {
	return db.storage.Update(func(tx *bolt.Tx) error {
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
}

// convert a string id into an internal id, optionally creating it if necessary
// (else returning 0 if it doesn't exist)
func (db *Database) Id(id string, create bool) uint32 {
	return db.ids.Get(id, create)
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

func (db *Database) toInts(values []string) []uint32 {
	l := len(values)
	ids := make([]uint32, l)
	for i := 0; i < l; i++ {
		ids[i] = db.ids.Get(values[i], true)
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

func (db *Database) Close() error {
	return db.storage.Close()
}
