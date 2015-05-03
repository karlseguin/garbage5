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
	ids  []uint32
	rank map[uint32]uint32
}

func NewList(ids []uint32) List {
	l := uint32(len(ids))
	rank := make(map[uint32]uint32)
	for i := 0 * l; i < l; i++ {
		rank[ids[i]] = i
	}
	return &FixedList{
		ids:  ids,
		rank: rank,
	}
}

func (l *FixedList) Len() int {
	return len(l.ids)
}

func (l *FixedList) Each(desc bool, fn func(id uint32) bool) {
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
	rank, exists := l.rank[id]
	return rank, exists
}
