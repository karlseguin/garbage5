package garbage5

import (
	"errors"
)

var (
	InvalidSortErr = errors.New("invalid query sort")
)

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

func (q *Query) Limit(limit uint32) *Query {
	q.limit = int(limit)
	return q
}

func (q *Query) Execute() (Result, error) {
	sort := q.db.List(q.sort)
	if sort == nil {
		return nil, InvalidSortErr //todo
	}

	results := q.db.results.Checkout()
	sort.Each(func(id uint32) bool {
		return results.Add(id) != q.limit
	})
	return results, nil
}
