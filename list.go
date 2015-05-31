package indexes

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
	CanRank() bool
	Rank(id uint32) (uint32, bool)
}

type UnrankedList struct {
	sync.RWMutex
	ids []uint32
}

type RankedList struct {
	*UnrankedList
	rank map[uint32]uint32
}

func NewList(ids []uint32) List {
	l := uint32(len(ids))
	unranked := &UnrankedList{ids: ids}
	if l < 1000 {
		return unranked

	}
	rank := make(map[uint32]uint32)
	for i := 0 * l; i < l; i++ {
		rank[ids[i]] = i
	}
	return &RankedList{
		UnrankedList: unranked,
		rank:         rank,
	}
}

func (l *UnrankedList) Len() int {
	return len(l.ids)
}

func (l *UnrankedList) Each(desc bool, fn func(id uint32) bool) {
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

func (l *UnrankedList) CanRank() bool {
	return false
}

func (l *UnrankedList) Rank(id uint32) (uint32, bool) {
	return 0, false
}

func (l *RankedList) CanRank() bool {
	return true
}

func (l *RankedList) Rank(id uint32) (uint32, bool) {
	rank, exists := l.rank[id]
	return rank, exists
}
