package indexes

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	BUCKET_COUNT = 32
	BUCKET_MASK  = 15
)

var (
	nullItem = &Item{
		expires: time.Now().Add(time.Hour * 10000),
	}
	summaryRef = nullItem
)

type Fetcher interface {
	LoadNResources(n int) (map[Id][][]byte, error)
	Fill([]interface{}, map[Id]int, [][]byte, []string, bool) error
	Get(id Id, tpe string) ([]byte, bool)
}

type Cache struct {
	gcing   uint32
	size    int64
	max     int64
	fetcher Fetcher
	ttl     time.Duration
	buckets []*Bucket
}

type Bucket struct {
	sync.RWMutex
	lookup map[Id]*Item
}

type Item struct {
	expires time.Time
	Value
}

type Value struct {
	tpe     int
	payload []byte
}

func newCache(fetcher Fetcher, configuration *Configuration) (*Cache, error) {
	cache := &Cache{
		fetcher: fetcher,
		ttl:     configuration.cacheTTL,
		max:     configuration.cacheSize,
		buckets: make([]*Bucket, BUCKET_COUNT),
	}
	for i := 0; i < BUCKET_COUNT; i++ {
		cache.buckets[i] = &Bucket{
			lookup: make(map[Id]*Item),
		}
	}
	if configuration.cachePreload > 0 {
		values, err := fetcher.LoadNResources(configuration.cachePreload)
		if err != nil {
			return nil, err
		}
		for id, payload := range values {
			tpe := intern(string(payload[0]))
			cache.set(id, Value{tpe, payload[1]}, false)
			if payload[2] == nil {
				cache.bucket(id, true).set(id, summaryRef)
			} else {
				cache.set(id, Value{tpe, payload[2]}, true)
			}
		}
	}
	return cache, nil
}

func (c *Cache) Fill(result *NormalResult, detailed bool) error {
	missCount := 0
	miss := result.miss
	indexMap := make(map[Id]int, result.Len())
	types := result.types
	payloads := result.payloads
	for i, id := range result.Ids() {
		item := c.get(id, detailed)
		if item == nil {
			payloads[i] = nil
			types[i] = ""
			miss[missCount] = id
			indexMap[id] = i
			missCount++
		} else {
			payloads[i] = item.Value.payload
		}
	}

	if missCount > 0 {
		if err := c.fetcher.Fill(miss[:missCount], indexMap, payloads, types, detailed); err != nil {
			return err
		}
		for id, index := range indexMap {
			c.set(id, Value{intern(types[index]), payloads[index]}, detailed)
		}
	}
	return nil
}

func (c *Cache) Fetch(id Id, tpeStr string) []byte {
	return c.fetch(id, tpeStr, true)
}

func (c *Cache) fetch(id Id, tpeStr string, detailed bool) []byte {
	return c.fetchTyped(id, intern(tpeStr), tpeStr, detailed)
}

func (c *Cache) fetchTyped(id Id, tpe int, tpeStr string, detailed bool) []byte {
	item := c.get(id, detailed)
	if item != nil {
		if detailed == true && item == summaryRef {
			return c.fetchTyped(id, tpe, tpeStr, false)
		}
		if item.tpe != tpe {
			return nil
		}
		return item.Value.payload
	}
	fmt.Println(id, tpeStr)
	payload, detailed := c.fetcher.Get(id, tpeStr)
	if payload == nil {
		return nil
	}
	if detailed == false {
		c.bucket(id, true).set(id, summaryRef)
	}
	c.set(id, Value{tpe, payload}, detailed)
	return payload
}

func (c *Cache) get(id Id, detailed bool) *Item {
	bucket := c.bucket(id, detailed)
	item := bucket.get(id)
	if item == nil {
		return nil
	}
	if item.expires.After(time.Now()) {
		return item
	}
	if bucket.remove(id) == true {
		atomic.AddInt64(&c.size, -int64(len(item.Value.payload)))
	}
	return nil
}

func (c *Cache) set(id Id, value Value, detailed bool) {
	item := &Item{
		Value:   value,
		expires: time.Now().Add(c.ttl),
	}
	if c.bucket(id, detailed).set(id, item) == true {
		if atomic.AddInt64(&c.size, int64(len(value.payload))) >= c.max && atomic.CompareAndSwapUint32(&c.gcing, 0, 1) {
			go c.gc()
		}
	}
}

func (c *Cache) Remove(id Id) {
	freed := c.bucket(id, true).sizeAndRemove(id)
	freed += c.bucket(id, false).sizeAndRemove(id)
	if freed > 0 {
		atomic.AddInt64(&c.size, -freed)
	}
}

func (c *Cache) bucket(id Id, detailed bool) *Bucket {
	if detailed {
		return c.buckets[id&BUCKET_MASK]
	}
	return c.buckets[id&BUCKET_MASK+16]

}

func (c *Cache) gc() {
	defer atomic.StoreUint32(&c.gcing, 0)
	freed := int64(0)
	for i := 0; i < BUCKET_COUNT; i++ {
		freed += c.buckets[i].gc()
	}
	atomic.AddInt64(&c.size, -freed)
}

func (b *Bucket) get(id Id) *Item {
	b.RLock()
	value := b.lookup[id]
	b.RUnlock()
	return value
}

func (b *Bucket) remove(id Id) bool {
	b.Lock()
	_, exists := b.lookup[id]
	delete(b.lookup, id)
	b.Unlock()
	return exists
}

func (b *Bucket) sizeAndRemove(id Id) int64 {
	defer b.Unlock()
	b.Lock()
	item, exists := b.lookup[id]
	if !exists {
		return 0
	}
	return int64(len(item.Value.payload))
}

func (b *Bucket) set(id Id, item *Item) bool {
	b.Lock()
	_, exists := b.lookup[id]
	b.lookup[id] = item
	b.Unlock()
	return !exists
}

func (b *Bucket) gc() int64 {
	visited := 0
	oldest := nullItem
	var oldestId Id

	b.RLock()
	for id, item := range b.lookup {
		if item.expires.Before(oldest.expires) {
			oldestId = id
			oldest = item
		}
		if visited++; visited == 10 {
			break
		}
	}
	b.RUnlock()

	b.Lock()
	delete(b.lookup, oldestId)
	b.Unlock()
	return int64(len(oldest.Value.payload))
}
