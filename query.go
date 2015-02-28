package garbage5

type Query struct {
	sort  string
	limit int
	db    *Database
}

func NewQuery(sort string, db *Database) *Query {
	return &Query{
		db:    db,
		limit: 50,
		sort:  sort,
	}
}

func (q *Query) Query() Result {
	sort := q.db.List(q.sort)
	if sort == nil {
		return nil //todo
	}

	results := q.db.results.Checkout()
	sort.Each(func(id uint32) bool {
		return results.Add(id) != q.limit
	})
	return results
}
