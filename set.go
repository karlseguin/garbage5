package garbage5

import (
	"gopkg.in/karlseguin/intset.v1"
	"sync"
)

type Set interface {
	Lock()
	RLock()
	Unlock()
	RUnlock()
}

type FixedSet struct {
	sync.RWMutex
	ids *intset.Sized32
}

func NewSet(ids []uint32) Set {
	l := len(ids)
	set := intset.NewSized32(uint32(l))
	for i := 0; i < l; i++ {
		set.Set(ids[i])
	}
	return &FixedSet{
		ids: set,
	}
}
