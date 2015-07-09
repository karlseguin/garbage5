package indexes

import (
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
		value:   nil,
		expires: time.Now().Add(time.Hour * 10000),
	}
	summaryRef = nullItem
)

type Fetcher interface {
	LoadNResources(n int) (map[Id][]byte, error)
	Fill([]interface{}, [][]byte) error
	Get(id Id) ([]byte, bool)
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
	value   []byte
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
	values, err := fetcher.LoadNResources(configuration.cachePreload)
	if err != nil {
		return nil, err
	}
	for id, payload := range values {
		cache.Set(id, payload, false)
	}
	return cache, nil
}

func (c *Cache) Fill(result *NormalResult) error {
	missCount := 0
	misses := result.misses
	payloads := result.payloads
	for i, id := range result.Ids() {
		resource := c.get(id, false)
		if resource == nil {
			misses[missCount] = i
			missCount++
			misses[missCount] = id
			missCount++
		} else {
			payloads[i] = resource
		}
	}
	if missCount > 0 {
		if err := c.fetcher.Fill(misses[:missCount], payloads); err != nil {
			return err
		}
		for i := 0; i < missCount; i += 2 {
			c.Set(misses[i+1].(Id), payloads[misses[i].(int)], false)
		}
	}
	return nil
}

func (c *Cache) Fetch(id Id) []byte {
	return c.fetch(id, true)
}

func (c *Cache) fetch(id Id, detailed bool) []byte {
	item := c.getItem(id, detailed)
	if item != nil {
		if detailed == true && item == summaryRef {
			return c.fetch(id, false)
		}
		return item.value
	}

	payload, detailed := c.fetcher.Get(id)
	if payload == nil {
		return nil
	}
	if detailed == false {
		c.bucket(id, true).set(id, summaryRef)
	}
	c.Set(id, payload, detailed)
	return payload
}

func (c *Cache) getItem(id Id, detailed bool) *Item {
	bucket := c.bucket(id, detailed)
	item := bucket.get(id)
	if item == nil {
		return nil
	}
	if item.expires.After(time.Now()) {
		return item
	}
	if bucket.remove(id) == true {
		atomic.AddInt64(&c.size, -int64(len(item.value)))
	}
	return nil
}

func (c *Cache) get(id Id, detailed bool) []byte {
	item := c.getItem(id, detailed)
	if item == nil {
		return nil
	}
	return item.value
}

func (c *Cache) Set(id Id, value []byte, detailed bool) {
	item := &Item{
		value:   value,
		expires: time.Now().Add(c.ttl),
	}
	if c.bucket(id, detailed).set(id, item) == true {
		if atomic.AddInt64(&c.size, int64(len(value))) >= c.max && atomic.CompareAndSwapUint32(&c.gcing, 0, 1) {
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
	return int64(len(item.value))
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
	return int64(len(oldest.value))
}
