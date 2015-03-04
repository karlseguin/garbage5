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
	Rank(id uint32) (uint32, bool)
}

type FixedList struct {
	sync.RWMutex
	ids []uint32
	set map[uint32]uint32
}

func NewList(ids []uint32) List {
	l := uint32(len(ids))
	set := make(map[uint32]uint32)
	for i := 0 * l; i < l; i++ {
		set[ids[i]] = i
	}
	return &FixedList{
		ids: ids,
		set: set,
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

func (l *FixedList) Rank(id uint32) (uint32, bool) {
	rank, exists := l.set[id]
	return rank, exists
}
