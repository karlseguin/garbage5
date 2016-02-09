package indexes

import "sort"

var (
	QueryPoolSize    = 64
	SmallSetTreshold = 500
)

type QueryPool chan *Query

type Filter func(id Id) bool

func NewQueryPool(db *Database, maxSets int, maxResults int) QueryPool {
	pool := make(QueryPool, QueryPoolSize)
	for i := 0; i < QueryPoolSize; i++ {
		result := newResult(maxSets, maxResults)
		query := &Query{
			db:     db,
			limit:  50,
			result: result,
			sets:   NewSets(maxSets),
		}
		result.query = query
		pool <- query
	}
	return pool
}

func (p QueryPool) Checkout() *Query {
	return <-p
}

type Query struct {
	limit  int
	around Id
	offset int
	sort   List
	desc   bool
	sets   *Sets
	db     *Database
	result *NormalResult
}

func (q *Query) Sort(name string) *Query {
	q.sort = q.db.GetList(name)
	return q
}

func (q *Query) SortAnd(name string) *Query {
	if q.sort != nil {
		q.AndSet(q.sort)
	}
	return q.Sort(name)
}

func (q *Query) SortList(list List) *Query {
	q.sort = list
	return q
}

// Specify the offset to start fetching results at
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

// Specify the maximum number of results to return
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

func (q *Query) Desc() *Query {
	q.desc = true
	return q
}

func (q *Query) Around(id Id) *Query {
	q.around = id
	return q
}

//apply the set to the result
func (q *Query) And(set string) *Query {
	return q.AndSet(q.db.GetSet(set))
}

func (q *Query) AndSet(set Set) *Query {
	q.sets.Add(set)
	return q
}

func (q *Query) HasSort() bool {
	return q.sort != nil
}

// Executes the query. After execution, the query object should not be used until
// Release() is called on the returned result
func (q *Query) Execute() (Result, error) {
	if q.limit == 0 {
		q.result.Release()
		return EmptyResult, nil
	}

	q.sets.RLock()
	defer q.sets.RUnlock()
	q.sets.Sort()

	if q.sort == nil {
		if q.sets.l == 0 {
			q.result.Release()
			return EmptyResult, nil
		}
		q.sort = q.sets.Shift()
	}

	if q.sort.Len() == 0 {
		q.result.Release()
		return EmptyResult, nil
	}

	l := q.sets.l
	if l == 0 {
		return q.execute(noFilter)
	}

	sl := q.sets.s[0].Len()
	if sl == 0 {
		q.result.Release()
		return EmptyResult, nil
	}

	q.sort.RLock()
	defer q.sort.RUnlock()

	if sl < SmallSetTreshold && q.sort.Len() > 1000 && q.sort.CanRank() && q.around == 0 {
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
	case 5:
		return q.fiveSetsFilter(start)
	default:
		return q.multiSetsFilter(start)
	}
}

func noFilter(id Id) bool {
	return true
}

func (q *Query) oneSetFilter(start int) Filter {
	s1 := q.sets.s[start]
	return func(id Id) bool {
		return s1.Exists(id)
	}
}

func (q *Query) twoSetsFilter(start int) Filter {
	s1, s2 := q.sets.s[start], q.sets.s[start+1]
	return func(id Id) bool {
		return s1.Exists(id) && s2.Exists(id)
	}
}

func (q *Query) threeSetsFilter(start int) Filter {
	s1, s2, s3 := q.sets.s[start], q.sets.s[start+1], q.sets.s[start+2]
	return func(id Id) bool {
		return s1.Exists(id) && s2.Exists(id) && s3.Exists(id)
	}
}

func (q *Query) fourSetsFilter(start int) Filter {
	s1, s2, s3, s4 := q.sets.s[start], q.sets.s[start+1], q.sets.s[start+2], q.sets.s[start+3]
	return func(id Id) bool {
		return s1.Exists(id) && s2.Exists(id) && s3.Exists(id) && s4.Exists(id)
	}
}

func (q *Query) fiveSetsFilter(start int) Filter {
	s1, s2, s3, s4, s5 := q.sets.s[start], q.sets.s[start+1], q.sets.s[start+2], q.sets.s[start+3], q.sets.s[start+4]
	return func(id Id) bool {
		return s1.Exists(id) && s2.Exists(id) && s3.Exists(id) && s4.Exists(id) && s5.Exists(id)
	}
}

func (q *Query) multiSetsFilter(start int) Filter {
	return func(id Id) bool {
		for i := start; i < q.sets.l; i++ {
			if q.sets.s[i].Exists(id) == false {
				return false
			}
		}
		return true
	}
}

//TODO: if len(q.sets) == 0, we could skip directly to the offset....
func (q *Query) execute(filter func(id Id) bool) (Result, error) {
	if q.around != 0 {
		q.limit = 1
		q.offset = 0
		q.sort.Around(q.around, func(id Id) bool {
			q.executeOne(filter, id)
			if q.limit == 0 {
				q.limit = 1
				return true
			}
			return false
		})
	} else {
		q.sort.Each(q.desc, func(id Id) bool {
			return q.executeOne(filter, id)
		})
	}
	return q.result, nil
}

func (q *Query) executeOne(filter func(id Id) bool, id Id) bool {
	if filter(id) == false {
		return true
	}
	if q.offset > 0 {
		q.offset--
	} else {
		if q.limit == 0 {
			q.result.more = true
			return false
		}
		q.result.add(id)
		q.limit--
	}
	return true
}

func (q *Query) setExecute(filter Filter) (Result, error) {
	set := q.sets.s[0]
	set.Each(true, func(id Id) bool {
		if filter(id) == false {
			return true
		}
		if rank, ok := q.sort.Rank(id); ok {
			q.result.addranked(id, rank)
		}
		return true
	})
	l := q.result.length
	q.result.length = 0
	ranks := q.result.ranked[:l]
	sort.Sort(ranks)

	if q.desc {
		for i := len(ranks) - q.offset - 1; i > -1; i-- {
			if q.setExecuteAdd(q.result, ranks[i].id) == false {
				break
			}
		}
	} else {
		for i, l := q.offset, len(ranks); i < l; i++ {
			if q.setExecuteAdd(q.result, ranks[i].id) == false {
				break
			}
		}
	}
	return q.result, nil
}

func (q *Query) setExecuteAdd(result *NormalResult, id Id) bool {
	if q.limit == 0 {
		result.more = true
		return false
	}
	result.add(id)
	q.limit--
	return true
}

// called when the result is released
func (q *Query) release() {
	q.sets.reset()
	q.sort = nil
	q.offset = 0
	q.around = 0
	q.limit = 50
	q.desc = false
	q.db.queries <- q
}
