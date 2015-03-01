package cache

import (
	"sync"
)

type bucket struct {
	sync.RWMutex
	lookup map[uint32]*Entry
}

func (b *bucket) get(id uint32) *Entry {
	defer b.RUnlock()
	b.RLock()
	return b.lookup[id]
}

func (b *bucket) set(id uint32, entry *Entry) *Entry {
	defer b.Unlock()
	b.Lock()
	existing := b.lookup[id]
	b.lookup[id] = entry
	return existing
}

func (b *bucket) delete(id uint32) *Entry {
	defer b.Unlock()
	b.Lock()
	existing := b.lookup[id]
	delete(b.lookup, id)
	return existing
}
