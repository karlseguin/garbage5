package garbage5

import (
	"sort"
)

type QueryPool struct {
	list chan *Query
}

type Filter func(id uint32) bool

func NewQueryPool(db *Database, maxSets int) *QueryPool {
	pool := &QueryPool{
		list: make(chan *Query, 64),
	}
	for i := 0; i < 64; i++ {
		pool.list <- &Query{
			db:    db,
			limit: 50,
			sets:  &Sets{s: make([]Set, maxSets)},
		}
	}
	return pool
}

func (p *QueryPool) Checkout(sort List) *Query {
	q := <-p.list
	q.sort = sort
	return q
}

type Query struct {
	limit  int
	offset int
	sort   List
	desc   bool
	sets   *Sets
	db     *Database
}

// Specify the offset to start fetching results at
func (q *Query) Offset(offset uint32) *Query {
	q.offset = int(offset)
	return q
}

// Specify the maximum number of results to return
func (q *Query) Limit(limit uint32) *Query {
	q.limit = int(limit)
	return q
}

func (q *Query) Desc() *Query {
	q.desc = true
	return q
}

//apply the set to the result
func (q *Query) And(set string) *Query {
	q.sets.Add(q.db.GetSet(set))
	return q
}

// Executes the query. After execution, the query object should not be used
func (q *Query) Execute() Result {
	defer q.Release()
	if q.sort == nil || q.limit == 0 {
		return EmptyResult
	}
	l := q.sets.l
	if l == 0 {
		return q.execute(noFilter)
	}

	q.sets.RLock()
	defer q.sets.RUnlock()
	q.sets.Sort()

	sl := q.sets.s[0].Len()
	if sl == 0 {
		return EmptyResult
	}

	q.sort.RLock()
	defer q.sort.RUnlock()
	if sl < SmallSetTreshold && q.sort.Len() > 1000 {
		return q.setExecute(q.getFilter(l, 1))
	}
	return q.execute(q.getFilter(l, 0))
}

func (q *Query) getFilter(count int, start int) Filter {
	switch count - start {
	case 0:
		return noFilter
	case 1:
		return q.oneSetFilter(start)
	case 2:
		return q.twoSetsFilter(start)
	case 3:
		return q.threeSetsFilter(start)
	case 4:
		return q.fourSetsFilter(start)
	default:
		return q.multiSetsFilter(start)
	}
}

func noFilter(id uint32) bool {
	return true
}

func (q *Query) oneSetFilter(start int) Filter {
	return func(id uint32) bool {
		return q.sets.s[start].Exists(id)
	}
}

func (q *Query) twoSetsFilter(start int) Filter {
	return func(id uint32) bool {
		return q.sets.s[start].Exists(id) && q.sets.s[start+1].Exists(id)
	}
}

func (q *Query) threeSetsFilter(start int) Filter {
	return func(id uint32) bool {
		return q.sets.s[start].Exists(id) && q.sets.s[start+1].Exists(id) && q.sets.s[start+2].Exists(id)
	}
}

func (q *Query) fourSetsFilter(start int) Filter {
	return func(id uint32) bool {
		return q.sets.s[start].Exists(id) && q.sets.s[start+1].Exists(id) && q.sets.s[start+2].Exists(id) && q.sets.s[start+3].Exists(id)
	}
}

func (q *Query) multiSetsFilter(start int) Filter {
	return func(id uint32) bool {
		for i := start; i < q.sets.l; i++ {
			if q.sets.s[i].Exists(id) == false {
				return false
			}
		}
		return true
	}
}

//TODO: if len(q.sets) == 0, we could skip directly to the offset....
func (q *Query) execute(filter func(id uint32) bool) Result {
	result := q.db.results.Checkout()
	q.sort.Each(q.desc, func(id uint32) bool {
		if q.limit == 0 {
			result.more = true
			return false
		}
		resource := q.db.getResource(id)
		if resource == nil {
			return true
		}
		if q.offset == 0 {
			if filter(id) {
				result.Add(id, resource)
				q.limit--
			}
		} else {
			q.offset--
		}
		return true
	})
	return result
}

func (q *Query) setExecute(filter Filter) Result {
	set := q.sets.s[0]
	result := q.db.results.Checkout()
	set.Each(func(id uint32) {
		if filter(id) {
			if rank, ok := q.sort.Rank(id); ok {
				result.AddRanked(id, rank)
			}
		}
	})
	ranks := result.ranked[:result.length]
	sort.Sort(ranks)
	result.length = 0

	if q.desc {
		for i := len(ranks) - q.offset - 1; i != -1; i-- {
			id := ranks[i].id
			if resource := q.db.getResource(id); resource != nil {
				result.Add(id, resource)
				q.limit--
				if q.limit == 0 {
					break
				}
			}
		}
	} else {
		for i, l := q.offset, len(ranks); i < l; i++ {
			id := ranks[i].id
			if resource := q.db.getResource(id); resource != nil {
				result.Add(id, resource)
				q.limit--
				if q.limit == 0 {
					break
				}
			}
		}
	}
	return result
}

func (q *Query) Release() {
	q.offset = 0
	q.limit = 50
	q.sets.l = 0
	q.desc = false
	q.db.queries.list <- q
}
