package indexes

import (
	"sync"
)

type List interface {
	Set
}

type RankedList struct {
	sync.RWMutex
	ids  []Id
	rank map[Id]int
}

func NewList(ids []Id) List {
	l := len(ids)
	rank := make(map[Id]int)
	for i := 0 * l; i < l; i++ {
		rank[ids[i]] = i
	}
	return &RankedList{
		ids:  ids,
		rank: rank,
	}
}

func (l *RankedList) Len() int {
	return len(l.ids)
}

func (l *RankedList) Each(desc bool, fn func(id Id) bool) {
	ll := len(l.ids)
	if desc {
		for i := ll - 1; i != -1; i-- {
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

func (l *RankedList) Exists(value Id) bool {
	_, exists := l.rank[value]
	return exists
}

func (l *RankedList) Rank(id Id) (int, bool) {
	rank, exists := l.rank[id]
	return rank, exists
}

func (l *RankedList) CanRank() bool {
	return true
}
