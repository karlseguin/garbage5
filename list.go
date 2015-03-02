package garbage5

import (
	"sync"
)

type List interface {
	Lock()
	RLock()
	Unlock()
	RUnlock()
	Len() int
	Each(func(id uint32) bool)
}

type FixedList struct {
	sync.RWMutex
	ids []uint32
}

func NewList(ids []uint32) List {
	return &FixedList{
		ids: ids,
	}
}

func (l *FixedList) Len() int {
	return len(l.ids)
}

func (l *FixedList) Each(fn func(id uint32) bool) {
	defer l.RUnlock()
	l.RLock()
	ll := len(l.ids)
	for i := 0; i < ll; i++ {
		if fn(l.ids[i]) == false {
			return
		}
	}
}
