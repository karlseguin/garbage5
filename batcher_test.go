package indexes

import (
	"database/sql"
	"testing"

	. "github.com/karlseguin/expect"
)

type BatcherTest struct {
}

func Test_Batcher(t *testing.T) {
	Expectify(&BatcherTest{}, t)
}

func (_ BatcherTest) InjectsSizeOf1IfNotSpecified() {
	conn := MustOpenSQL()
	b := MustBatcher(conn, "select id, summary from resources where id in #IN#")
	q := b.For([]interface{}{3, 5})
	assertBatchQuery(q, 3)
	assertBatchQuery(q, 5)
	Expect(q.HasMore()).To.Equal(false)
}

func (_ BatcherTest) Batchesqueries() {
	conn := MustOpenSQL()
	b := MustBatcher(conn, "select id, summary from resources where id in #IN#", 4, 2, 1)
	q := b.For([]interface{}{1, 2, 3, 4, 5, 6, 7})
	assertBatchQuery(q, 1, 2, 3, 4)
	assertBatchQuery(q, 5, 6)
	assertBatchQuery(q, 7)
	Expect(q.HasMore()).To.Equal(false)
}

func MustOpenSQL() *sql.DB {
	conn, err := sql.Open("sqlite3", "test.db")
	if err != nil {
		panic(err)
	}
	return conn
}

func assertBatchQuery(q *BatchQuery, expected ...int) {
	Expect(q.HasMore()).To.Equal(true)
	actuals := make([]int, 0, len(expected))

	rows, err := q.Query()
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var summary []byte
		rows.Scan(&id, &summary)
		actuals = append(actuals, id)
	}
	Expect(len(actuals)).To.Equal(len(expected))
	for i, id := range actuals {
		Expect(id).To.Equal(expected[i])
	}
}
