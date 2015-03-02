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
			db:   db,
			sets: make([]Set, 32),
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
	if s := q.db.GetSet(set); s != nil {
		q.sets[q.setIndex] = s
		q.setIndex++
	}
	return q
}

// Executethe query
func (q *Query) Execute() Result {
	if q.sort == nil {
		return EmptyResult
	}
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
			result.Add(id, resource)
			q.limit--
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
