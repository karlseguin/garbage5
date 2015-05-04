package garbage5

import (
	"encoding/binary"
	"github.com/karlseguin/bolt"
	"github.com/karlseguin/garbage5/cache"
	"gopkg.in/karlseguin/bytepool.v3"
	"sync"
)

var (
	Endianness       = binary.LittleEndian
	IDS_BUCKET       = []byte("ids")
	SETS_BUCKET      = []byte("sets")
	LISTS_BUCKET     = []byte("lists")
	RESOURCES_BUCKET = []byte("resources")
	bp               = bytepool.NewEndian(65536, 64, Endianness)
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
	cache    *cache.Cache
	sets     map[string]Set
	lists    map[string]List
	queries  QueryPool
}

func New(c *Configuration) (*Database, error) {
	db, err := bolt.Open(c.path, 0600, nil)
	if err != nil {
		return nil, err
	}
	database := &Database{
		storage: db,
		sets:    make(map[string]Set),
		lists:   make(map[string]List),
		cache:   cache.New(c.cacheSize),
	}
	database.queries = NewQueryPool(database, c.maxSets, c.maxResults)
	database.ids = NewIdMap(database.writeId)

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
			db.ids.load(string(k), Endianness.Uint32(v))
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
// todo: conditionally update the cache
func (db *Database) PutResource(resource Resource) error {
	id, bytes := resource.Id(), resource.Bytes()
	internal, encoded := db.ids.Internal(id)

	err := db.storage.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(RESOURCES_BUCKET).Put(encoded, bytes)
	})
	if err != nil {
		return err
	}
	db.cache.Update(internal, bytes)
	return nil
}

// TODO: reduce allocations
// Since getResource is always cached, we're limited in our ability to reduce
// allocation. The cache also reduces our need...but...still.
// We can't use a pool. We could over-allocate outside of the transaction
// (which would be nice), but we'd probably want to trim it since it's long-lived
// and we don't want to waste the space.
func (db *Database) getResource(id uint32) []byte {
	resource := db.cache.Get(id)
	if resource != nil {
		return resource
	}
	encoded := db.ids.Bytes(id)
	db.storage.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(RESOURCES_BUCKET).Get(encoded)
		if value != nil {
			resource = make([]byte, len(value))
			copy(resource, value)
		}
		return nil
	})
	db.cache.Set(id, resource)
	return resource
}

func (db *Database) Query(sort string) *Query {
	return db.queries.Checkout(db.GetList(sort))
}

// Close the database
func (db *Database) Close() error {
	return db.storage.Close()
}

func (db *Database) writeIds(bucket []byte, name string, ids []string) ([]uint32, error) {
	l := len(ids)
	internals := make([]uint32, l)

	buffer := bp.Checkout()
	defer buffer.Release()
	buffer.WriteUint32(uint32(l))

	return internals, db.storage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(IDS_BUCKET)
		for i := 0; i < l; i++ {
			id := ids[i]
			internal, bytes := db.ids.InternalWrite(id, func(k, v []byte) {
				b.Put(k, v)
			})
			internals[i] = internal
			buffer.Write(bytes)
		}
		return tx.Bucket(bucket).Put([]byte(name), buffer.Bytes())
	})
}

func (db *Database) loadLists(bucket []byte, fn func(name string, ids []uint32)) {
	db.storage.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			l := Endianness.Uint32(v)
			ids := make([]uint32, l)
			for i := 0 * l; i < l; i++ {
				start := (i + 1) * 4
				ids[i] = Endianness.Uint32(v[start:])
			}
			fn(string(k), ids)
		}
		return nil
	})
}

func (db *Database) writeId(key, value []byte) {
	db.storage.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(IDS_BUCKET).Put(key, value)
	})
}
