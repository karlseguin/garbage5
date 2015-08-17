package indexes

import (
	"database/sql"
	"sort"
	"strings"
)

type BatchMiss struct {
	ids     []Id
	indexes []int
	params  []interface{}
}

type Batcher struct {
	sizes []int
	stmts []*sql.Stmt
}

func MustBatcher(conn *sql.DB, query string, sizes ...int) Batcher {
	batcher, err := NewBatcher(conn, query, sizes...)
	if err != nil {
		panic(err)
	}
	return batcher
}

func NewBatcher(conn *sql.DB, query string, sizes ...int) (Batcher, error) {
	l := len(sizes)
	sort.Reverse(sort.IntSlice(sizes))
	if l == 0 || sizes[l-1] != 1 {
		sizes = append(sizes, 1)
		l++
	}

	s := make([]int, l)
	stmts := make([]*sql.Stmt, l)

	for i, size := range sizes {
		placeholders := strings.Repeat("?,", size)
		stmt, err := conn.Prepare(strings.Replace(query, "#IN#", "("+placeholders[:len(placeholders)-1]+")", 1))
		if err != nil {
			return Batcher{}, err
		}
		s[i] = size
		stmts[i] = stmt
	}
	return Batcher{s, stmts}, nil
}

func (b Batcher) For(values []interface{}) *BatchQuery {
	return &BatchQuery{b, values}
}

type BatchQuery struct {
	batcher Batcher
	values  []interface{}
}

func (bq BatchQuery) HasMore() bool {
	return len(bq.values) > 0
}

func (bq *BatchQuery) Query() (*sql.Rows, error) {
	l := len(bq.values)
	var index, size int

	for index, size = range bq.batcher.sizes {
		if size <= l {
			break
		}
	}

	rows, err := bq.batcher.stmts[index].Query(bq.values[:size]...)
	bq.values = bq.values[size:]
	return rows, err
}
