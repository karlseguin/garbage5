package garbage5

import (
	"sync"
)

type IdMap struct {
	sync.RWMutex
	counter uint32
	etoi    map[string]uint32
	itoe    map[uint32]string
	itob    map[uint32][]byte
	writer  func(k, v []byte)
}

func NewIdMap(writer func(k, v []byte)) *IdMap {
	return &IdMap{
		writer: writer,
		etoi:   make(map[string]uint32),
		itoe:   make(map[uint32]string),
		itob:   make(map[uint32][]byte),
	}
}

// not thread safe. Call on init and never again
func (m *IdMap) load(external string, internal uint32) {
	m.etoi[external] = internal
	m.itoe[internal] = external
	m.itob[internal] = encodeId(internal)
	if internal > m.counter {
		m.counter = internal
	}
}

func (m *IdMap) Internal(external string) (uint32, []byte) {
	return m.InternalWrite(external, m.writer)
}

func (m *IdMap) InternalWrite(external string, writer func(k, v []byte)) (uint32, []byte) {
	m.RLock()
	internal, exists := m.etoi[external]
	if exists {
		bytes := m.itob[internal]
		m.RUnlock()
		return internal, bytes
	}
	m.RUnlock()

	m.Lock()
	internal, exists = m.etoi[external]
	var bytes []byte
	if exists == false {
		m.counter++
		internal = m.counter
		m.etoi[external] = internal
		m.itoe[internal] = external
		bytes = encodeId(internal)
		m.itob[internal] = bytes
	} else {
		bytes = m.itob[internal]
	}

	m.Unlock()
	if exists == false {
		writer([]byte(external), bytes)
	}
	return internal, bytes
}

func (m *IdMap) Bytes(internal uint32) []byte {
	defer m.RUnlock()
	m.RLock()
	return m.itob[internal]
}

func (m *IdMap) External(internal uint32) string {
	defer m.RUnlock()
	m.RLock()
	return m.itoe[internal]
}

func encodeId(id uint32) []byte {
	encoded := make([]byte, 4)
	Endianness.PutUint32(encoded, id)
	return encoded
}
