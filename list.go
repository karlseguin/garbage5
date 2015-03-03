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
	Each(bool, func(id uint32) bool)
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

func (l *FixedList) Each(desc bool, fn func(id uint32) bool) {
	defer l.RUnlock()
	l.RLock()
	ll := len(l.ids) - 1
	if desc {
		for i := ll; i != -1; i-- {
			if fn(l.ids[i]) == false {
				return
			}
		}
	} else {
		for i := 0; i != ll; i++ {
			if fn(l.ids[i]) == false {
				return
			}
		}
	}
}
