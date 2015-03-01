package garbage5

import (
	"encoding/binary"
	"github.com/karlseguin/bolt"
	"gopkg.in/karlseguin/bytepool.v3"
	"sync"
)

var (
	IDS_BUCKET       = []byte("ids")
	SETS_BUCKET      = []byte("sets")
	LISTS_BUCKET     = []byte("lists")
	RESOURCES_BUCKET = []byte("resources")
	bp               = bytepool.NewEndian(65536, 64, binary.LittleEndian)
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
		if _, err := tx.CreateBucketIfNotExists(IDS_BUCKET); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(SETS_BUCKET); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(LISTS_BUCKET); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(RESOURCES_BUCKET); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	db.storage.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(IDS_BUCKET).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			db.ids.load(string(k), binary.LittleEndian.Uint32(v))
		}
		return nil
	})

	db.loadLists(SETS_BUCKET, func(name string, ids []uint32) {
		db.sets[name] = NewSet(ids)
	})
	db.loadLists(LISTS_BUCKET, func(name string, ids []uint32) {
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
	internal, err := db.writeIds(LISTS_BUCKET, name, ids)
	if err != nil {
		return err
	}
	list := NewList(internal)
	db.listLock.Lock()
	db.lists[name] = list
	db.listLock.Unlock()
	return nil
}

// Creates, or overwirtes, an in-memory and on-disk set
func (db *Database) CreateSet(name string, ids ...string) error {
	internal, err := db.writeIds(SETS_BUCKET, name, ids)
	if err != nil {
		return err
	}
	set := NewSet(internal)
	db.setLock.Lock()
	db.sets[name] = set
	db.setLock.Unlock()
	return nil
}

// Store a resource
func (db *Database) Put(resource Resource) error {
	return db.storage.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(RESOURCES_BUCKET).Put([]byte(resource.Id()), resource.Bytes())
	})
}

func (db *Database) Query(sort string) *Query {
	return NewQuery(sort, db)
}

// Close the database
func (db *Database) Close() error {
	return db.storage.Close()
}

func (db *Database) writeIds(bucket []byte, name string, ids []string) ([]uint32, error) {
	l := len(ids)
	internals := make([]uint32, l)
	newIds := make(map[string][]byte)

	buffer := bp.Checkout()
	defer buffer.Release()
	buffer.WriteUint32(uint32(l))

	for i := 0; i < l; i++ {
		id := ids[i]
		internal, isNew := db.ids.Internal(id, true)
		internals[i] = internal
		if isNew {
			encoded := make([]byte, 4)
			binary.LittleEndian.PutUint32(encoded, internal)
			newIds[id] = encoded
			buffer.Write(encoded)
		} else {
			buffer.WriteUint32(internal)
		}
	}

	return internals, db.storage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(IDS_BUCKET)
		for id, internal := range newIds {
			if err := b.Put([]byte(id), internal); err != nil {
				return err
			}
		}
		return tx.Bucket(bucket).Put([]byte(name), buffer.Bytes())
	})
}

func (db *Database) loadLists(bucket []byte, fn func(name string, ids []uint32)) {
	db.storage.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			l := binary.LittleEndian.Uint32(v)
			ids := make([]uint32, l)
			for i := 0 * l; i < l; i++ {
				start := (i + 1) * 4
				ids[i] = binary.LittleEndian.Uint32(v[start:])
			}
			fn(string(k), ids)
		}
		return nil
	})
}
