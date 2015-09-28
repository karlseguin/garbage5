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
	Around(id Id, f func(id Id) bool)
}

type Sets struct {
	l int
	s []Set
}

func (sets *Sets) Add(set Set) {
	sets.s[sets.l] = set
	sets.l++
}

func (sets *Sets) Shift() Set {
	set := sets.s[0]
	sets.s = sets.s[1:]
	sets.l--
	return set
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

func NewSet(ids []Id) Set {
	l := len(ids)
	if l < 32 {
		return NewSmallSet(ids)
	}

	set := intset.NewSized32(uint32(l))
	for i := 0; i < l; i++ {
		set.Set(uint32(ids[i]))
	}
	return &FixedSet{
		ids: set,
	}
}

type FixedSet struct {
	sync.RWMutex
	ids *intset.Sized32
}

func (s *FixedSet) Len() int {
	return s.ids.Len()
}

func (s *FixedSet) Exists(value Id) bool {
	return s.ids.Exists(uint32(value))
}

func (s *FixedSet) Each(desc bool, fn func(Id) bool) {
	s.ids.Each(func(id uint32) {
		if !fn(Id(id)) {
			return
		}
	})
}

// cannot be done
func (s *FixedSet) Around(id Id, fn func(Id) bool) {
	s.Each(false, fn)
}

func (s *FixedSet) CanRank() bool {
	return false
}

func (s *FixedSet) Rank(id Id) (int, bool) {
	return 0, false
}

type SmallSet struct {
	sync.RWMutex
	ids []Id
}

func NewSmallSet(ids []Id) *SmallSet {
	return &SmallSet{ids: sortIds(ids)}
}

func (s *SmallSet) Len() int {
	return len(s.ids)
}

func (s *SmallSet) Exists(value Id) bool {
	for _, id := range s.ids {
		if id > value {
			return false
		}
		if value == id {
			return true
		}
	}
	return false
}

func (s *SmallSet) Each(dec bool, fn func(Id) bool) {
	for _, id := range s.ids {
		if !fn(id) {
			return
		}
	}
}

func (s *SmallSet) CanRank() bool {
	return false
}

func (s *SmallSet) Rank(id Id) (int, bool) {
	return 0, false
}

func (s *SmallSet) Around(id Id, fn func(id Id) bool) {
	s.Each(false, fn)
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

func (s *emptySet) Around(id Id, fn func(Id) bool) {

}

func (s *emptySet) CanRank() bool {
	return false
}

func (s *emptySet) Rank(id Id) (int, bool) {
	return 0, false
}

func sortIds(ids []Id) []Id {
	l := len(ids)
	for i := 1; i < l; i++ {
		j := i
		t := ids[i]
		for ; j > 0 && ids[j-1] > t; j-- {
			ids[j] = ids[j-1]
		}
		ids[j] = t
	}
	return ids
}
