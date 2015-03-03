package garbage5

type QueryPool struct {
	list chan *Query
}

func NewQueryPool(db *Database) *QueryPool {
	pool := &QueryPool{
		list: make(chan *Query, 64),
	}
	for i := 0; i < 64; i++ {
		pool.list <- &Query{
			db:    db,
			limit: 50,
			sets:  make([]Set, 32),
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
	limit    int
	offset   int
	setIndex int
	sort     List
	sets     []Set
	db       *Database
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
	s := q.db.GetSet(set)
	q.sets[q.setIndex] = s
	q.setIndex++
	return q
}

// Executethe query
func (q *Query) Execute() Result {
	if q.sort == nil {
		return EmptyResult
	}
	l := q.setIndex
	if l == 0 {
		return q.execute(func(id uint32) bool { return true })
	}

	for i := 1; i < l; i++ {
		for j := i; j > 0 && q.sets[j-1].Len() > q.sets[j].Len(); j-- {
			q.sets[j], q.sets[j-1] = q.sets[j-1], q.sets[j]
		}
	}
	if q.sets[0].Len() == 0 {
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
	return q.sets[0].Exists(id)
}

func (q *Query) twoSetsFilter(id uint32) bool {
	return q.sets[0].Exists(id) && q.sets[1].Exists(id)
}

func (q *Query) threeSetsFilter(id uint32) bool {
	return q.sets[0].Exists(id) && q.sets[1].Exists(id) && q.sets[2].Exists(id)
}

func (q *Query) fourSetsFilter(id uint32) bool {
	return q.sets[0].Exists(id) && q.sets[1].Exists(id) && q.sets[2].Exists(id) && q.sets[3].Exists(id)
}

func (q *Query) multiSetsFilter(id uint32) bool {
	for i := 0; i < q.setIndex; i++ {
		if q.sets[i].Exists(id) == false {
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
	q.setIndex = 0
}
