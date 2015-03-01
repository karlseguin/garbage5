package garbage5

import (
	"sync"
)

type IdMap struct {
	sync.RWMutex
	counter uint32
	etoi    map[string]uint32
	itoe    map[uint32]string
}

func NewIdMap() *IdMap {
	return &IdMap{
		etoi: make(map[string]uint32),
		itoe: make(map[uint32]string),
	}
}

func (m *IdMap) Internal(external string, create bool) uint32 {
	m.RLock()
	internal, exists := m.etoi[external]
	m.RUnlock()
	if exists || create == false {
		return internal
	}

	defer m.Unlock()
	m.Lock()
	internal, exists = m.etoi[external]
	if exists == false {
		m.counter++
		internal = m.counter
		m.etoi[external] = internal
		m.itoe[internal] = external
	}
	return internal
}

func (m *IdMap) External(internal uint32) string {
	defer m.RUnlock()
	m.RLock()
	return m.itoe[internal]
}
