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

// Specify the maximum number of results to return
func (q *Query) Limit(limit uint32) *Query {
	q.limit = int(limit)
	return q
}

// Executethe query
func (q *Query) Execute() (Result, error) {
	sort := q.db.GetList(q.sort)
	if sort == nil {
		return nil, InvalidSortErr //todo
	}

	result := q.db.results.Checkout()
	count := 0
	sort.Each(func(id uint32) bool {
		if count == q.limit {
			result.more = true
			return false
		}
		if result.Add(id) {
			count++
		}
		return true
	})
	return result, nil
}
