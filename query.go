package garbage5

type QueryPool struct {
	list chan *Query
}

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

//apply the set to the result
func (q *Query) And(set string) *Query {
	q.sets.Add(q.db.GetSet(set))
	return q
}

// Executethe query
func (q *Query) Execute() Result {
	if q.sort == nil || q.limit == 0 {
		return EmptyResult
	}
	l := q.sets.l
	if l == 0 {
		return q.execute(func(id uint32) bool { return true })
	}

	q.sets.RLock()
	defer q.sets.RUnlock()
	q.sets.Sort()
	if q.sets.s[0].Len() == 0 {
		return EmptyResult
	}

	//TODO: optimize for when sets[0].Len() is much smaller than sort.Len()
	if l == 1 {
		return q.execute(q.oneSetFilter)
	}
	if l == 2 {
		return q.execute(q.twoSetsFilter)
	}
	if l == 3 {
		return q.execute(q.threeSetsFilter)
	}
	if l == 4 {
		return q.execute(q.fourSetsFilter)
	}
	return q.execute(q.multiSetsFilter)
}

func (q *Query) oneSetFilter(id uint32) bool {
	return q.sets.s[0].Exists(id)
}

func (q *Query) twoSetsFilter(id uint32) bool {
	return q.sets.s[0].Exists(id) && q.sets.s[1].Exists(id)
}

func (q *Query) threeSetsFilter(id uint32) bool {
	return q.sets.s[0].Exists(id) && q.sets.s[1].Exists(id) && q.sets.s[2].Exists(id)
}

func (q *Query) fourSetsFilter(id uint32) bool {
	return q.sets.s[0].Exists(id) && q.sets.s[1].Exists(id) && q.sets.s[2].Exists(id) && q.sets.s[3].Exists(id)
}

func (q *Query) multiSetsFilter(id uint32) bool {
	for i := 0; i < q.sets.l; i++ {
		if q.sets.s[i].Exists(id) == false {
			return false
		}
	}
	return true
}

func (q *Query) execute(filter func(id uint32) bool) Result {
	result := q.db.results.Checkout()
	q.sort.Each(func(id uint32) bool {
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

func (q *Query) Release() {
	q.offset = 0
	q.limit = 50
	q.sets.l = 0
}
