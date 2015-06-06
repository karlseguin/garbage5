package indexes

import (
	"sync"

	"gopkg.in/karlseguin/intset.v1"
)

var (
	EmptySet = new(emptySet)
)

type Set interface {
	Lock()
	RLock()
	Unlock()
	RUnlock()
	Len() int
	Exists(value Id) bool
	Each(bool, func(Id) bool)
	CanRank() bool
	Rank(id Id) (int, bool)
}

type Sets struct {
	l int
	s []Set
}

func (sets *Sets) Add(set Set) {
	sets.s[sets.l] = set
	sets.l++
}

func (sets *Sets) RLock() {
	for i := 0; i < sets.l; i++ {
		sets.s[i].RLock()
	}
}

func (sets *Sets) RUnlock() {
	for i := 0; i < sets.l; i++ {
		sets.s[i].RUnlock()
	}
}

// insertion sort
func (sets *Sets) Sort() {
	for i := 1; i < sets.l; i++ {
		j := i
		t := sets.s[i]
		l := t.Len()
		for ; j > 0 && sets.s[j-1].Len() > l; j-- {
			sets.s[j] = sets.s[j-1]
		}
		sets.s[j] = t
	}
}

type FixedSet struct {
	sync.RWMutex
	ids *intset.Sized32
}

func NewSet(ids []Id) Set {
	l := len(ids)
	set := intset.NewSized32(uint32(l))
	for i := 0; i < l; i++ {
		set.Set(uint32(ids[i]))
	}
	return &FixedSet{
		ids: set,
	}
}

func (s *FixedSet) Len() int {
	return s.ids.Len()
}

func (s *FixedSet) Exists(value Id) bool {
	return s.ids.Exists(uint32(value))
}

func (s *FixedSet) Each(desc bool, fn func(Id) bool) {
	s.ids.Each(func(id uint32) {
		fn(Id(id))
	})
}

func (s *FixedSet) CanRank() bool {
	return false
}

func (s *FixedSet) Rank(id Id) (int, bool) {
	return 0, false
}

type emptySet struct {
}

func (s *emptySet) Lock() {

}

func (s *emptySet) RLock() {

}

func (s *emptySet) Unlock() {

}

func (s *emptySet) RUnlock() {

}

func (s *emptySet) Len() int {
	return 0
}

func (s *emptySet) Exists(value Id) bool {
	return false
}

func (s *emptySet) Each(desc bool, fn func(Id) bool) {

}

func (s *emptySet) CanRank() bool {
	return false
}

func (s *emptySet) Rank(id Id) (int, bool) {
	return 0, false
}
