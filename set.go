package garbage5

import (
	"gopkg.in/karlseguin/intset.v1"
	"sync"
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
	Exists(value uint32) bool
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

func (sets *Sets) Sort() {
	for i := 1; i < sets.l; i++ {
		for j := i; j > 0 && sets.s[j-1].Len() > sets.s[j].Len(); j-- {
			sets.s[j], sets.s[j-1] = sets.s[j-1], sets.s[j]
		}
	}
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

func (s *FixedSet) Len() int {
	return s.ids.Len()
}

func (s *FixedSet) Exists(value uint32) bool {
	return s.ids.Exists(value)
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

func (s *emptySet) Exists(value uint32) bool {
	return false
}
