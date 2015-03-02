package garbage5

import (
	"errors"
)

var (
	InvalidSortErr = errors.New("invalid query sort")
)

type Query struct {
	sort   string
	limit  int
	offset int
	db     *Database
}

func NewQuery(sort string, db *Database) *Query {
	return &Query{
		db:    db,
		limit: 50,
		sort:  sort,
	}
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

// Executethe query
func (q *Query) Execute() (Result, error) {
	sort := q.db.GetList(q.sort)
	if sort == nil {
		return nil, InvalidSortErr
	}

	result := q.db.results.Checkout()
	sort.Each(func(id uint32) bool {
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
	return result, nil
}
