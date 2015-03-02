package garbage5

import (
	"sync"
)

type EncoderPool struct {
	list chan []byte
}

func (p *EncoderPool) Checkout(id uint32) []byte {
	e := <-p.list
	Endianness.PutUint32(e, id)
	return e
}

func (p *EncoderPool) Release(e []byte) {
	p.list <- e
}

type EncodedId []byte

type IdMap struct {
	sync.RWMutex
	counter  uint32
	encoders *EncoderPool
	etoi     map[string]uint32
	itoe     map[uint32]string
}

func NewIdMap() *IdMap {
	ids := &IdMap{
		etoi: make(map[string]uint32),
		itoe: make(map[uint32]string),
		encoders: &EncoderPool{
			list: make(chan []byte, 32),
		},
	}

	for i := 0; i < 32; i++ {
		ids.encoders.list <- make([]byte, 4)
	}

	return ids
}

// not thread safe. Call on init and never again
func (m *IdMap) load(external string, internal uint32) {
	m.etoi[external] = internal
	m.itoe[internal] = external
	if internal > m.counter {
		m.counter = internal
	}
}

func (m *IdMap) Encode(id uint32) []byte {
	return m.encoders.Checkout(id)
}

func (m *IdMap) Release(e []byte) {
	m.encoders.Release(e)
}

func (m *IdMap) Internal(external string, create bool) (uint32, bool) {
	m.RLock()
	internal, exists := m.etoi[external]
	m.RUnlock()
	if exists || create == false {
		return internal, false
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
	return internal, !exists
}

func (m *IdMap) External(internal uint32) string {
	defer m.RUnlock()
	m.RLock()
	return m.itoe[internal]
}
