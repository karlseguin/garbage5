package indexes

import "sync"

type List interface {
	Set
}

type RankedList struct {
	sync.RWMutex
	ids  []Id
	rank map[Id]Id
}

func NewList(ids []Id) List {
	l := len(ids)
	rank := make(map[Id]Id, l)
	for i := 0 * l; i < l; i++ {
		rank[ids[i]] = Id(i)
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
	if !desc {
		for _, id := range l.ids {
			if !fn(id) {
				return
			}
		}
		return
	}
	for i := len(l.ids) - 1; i != -1; i-- {
		if fn(l.ids[i]) == false {
			return
		}
	}
}

func (s RankedList) Around(target Id, fn func(Id) bool) {
	l := Id(len(s.ids))
	index := s.rank[target]

	for next := index + 1; next < l; next++ {
		if fn(s.ids[next]) {
			break
		}
	}

	//there can be no prev
	if index == 0 {
		return
	}

	prev := index - 1
	for {
		if fn(s.ids[prev]) {
			break
		}
		if prev == 0 {
			break
		}
		prev--
	}
}

func (l *RankedList) Exists(value Id) bool {
	_, exists := l.rank[value]
	return exists
}

func (l *RankedList) Rank(id Id) (int, bool) {
	rank, exists := l.rank[id]
	return int(rank), exists
}

func (l *RankedList) CanRank() bool {
	return true
}

type SimpleList []Id

func (s SimpleList) Lock() {
	// too simple!
}

func (s SimpleList) RLock() {
	// too simple!
}

func (s SimpleList) Unlock() {
	// too simple!
}

func (s SimpleList) RUnlock() {
	// too simple!
}

func (s SimpleList) Len() int {
	return len(s)
}

func (s SimpleList) Exists(value Id) bool {
	return false
}

func (s SimpleList) Each(desc bool, fn func(Id) bool) {
	if !desc {
		for _, id := range s {
			if !fn(id) {
				return
			}
		}
		return
	}

	for i := len(s) - 1; i != -1; i-- {
		if !fn(s[i]) {
			return
		}
	}
}

func (s SimpleList) Around(target Id, fn func(Id) bool) {
	s.Each(false, fn)
}

func (s SimpleList) CanRank() bool {
	return false
}

func (s SimpleList) Rank(id Id) (int, bool) {
	return -1, false
}
