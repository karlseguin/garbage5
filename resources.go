package indexes

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	BUCKET_COUNT = 16
	BUCKET_MASK  = BUCKET_COUNT - 1
)

var nullItem = &Item{
	id:      0,
	expires: time.Now().Add(time.Hour * 1000),
	value:   nil,
}

type Fetcher func([]*Miss) error

type Miss struct {
	id      Id
	index   int
	payload []byte
}

type Resources struct {
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
	id      Id
	expires time.Time
	value   []byte
}

func newResources(fetcher Fetcher, configuration *Configuration) *Resources {
	resources := &Resources{
		fetcher: fetcher,
		ttl:     configuration.cacheTTL,
		max:     configuration.cacheSize,
		buckets: make([]*Bucket, BUCKET_COUNT),
	}
	for i := 0; i < BUCKET_COUNT; i++ {
		resources.buckets[i] = &Bucket{
			lookup: make(map[Id]*Item),
		}
	}
	return resources
}

func (r *Resources) Fill(result *NormalResult) error {
	missCount := 0
	for i, id := range result.Ids() {
		resource := r.get(id)
		if resource == nil {
			miss := result.misses[missCount]
			miss.id, miss.index = id, i
			missCount++
		} else {
			result.payloads[i] = resource
		}
	}

	if missCount > 0 {
		misses := result.misses[:missCount]
		if err := r.fetcher(misses); err != nil {
			return err
		}
		for _, miss := range misses {
			result.payloads[miss.index] = miss.payload
			r.set(miss.id, miss.payload)
		}
	}
	return nil
}

func (r *Resources) get(id Id) []byte {
	bucket := r.bucket(id)
	item := bucket.get(id)
	if item == nil {
		return nil
	}
	if item.expires.After(time.Now()) {
		return item.value
	}
	if bucket.remove(id) == true {
		atomic.AddInt64(&r.size, int64(len(item.value)))
	}
	return nil
}

func (r *Resources) set(id Id, value []byte) {
	item := &Item{
		id:      id,
		expires: time.Now().Add(r.ttl),
		value:   value,
	}
	if r.bucket(id).set(id, item) == true {
		if atomic.AddInt64(&r.size, int64(len(value))) >= r.max && atomic.CompareAndSwapUint32(&r.gcing, 0, 1) {
			go r.gc()
		}
	}
}

func (r *Resources) bucket(id Id) *Bucket {
	return r.buckets[id&BUCKET_MASK]
}

func (r *Resources) gc() {
	defer atomic.StoreUint32(&r.gcing, 0)
	freed := int64(0)
	for i := 0; i < BUCKET_COUNT; i++ {
		freed += r.buckets[i].gc()
	}
	atomic.AddInt64(&r.size, -freed)
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

	b.RLock()
	for _, item := range b.lookup {
		if item.expires.Before(oldest.expires) {
			oldest = item
		}
		if visited++; visited == 10 {
			break
		}
	}
	b.RUnlock()

	b.Lock()
	delete(b.lookup, oldest.id)
	b.Unlock()
	return int64(len(oldest.value))
}
