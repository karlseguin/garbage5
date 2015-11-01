package indexes

import (
	"testing"

	. "github.com/karlseguin/expect"
)

type DatabaseTests struct {
}

func Test_Database(t *testing.T) {
	Expectify(&DatabaseTests{}, t)
}

func (_ DatabaseTests) ReloadLoadsANewList() {
	db := createDB()
	defer db.Close()
	res, _ := db.Query().Sort("late_list").Execute()
	Expect(res.Len()).To.Equal(0)

	fakeNewIndexes(db)
	db.Reload()
	res, _ = db.Query().Sort("late_list").Execute()
	Expect(res.Len()).To.Equal(2)
}

func (_ DatabaseTests) ReloadLoadsANewSet() {
	db := createDB()
	defer db.Close()
	res, _ := db.Query().And("late_set").Execute()
	Expect(res.Len()).To.Equal(0)

	fakeNewIndexes(db)
	db.Reload()
	res, _ = db.Query().And("late_set").Execute()
	Expect(res.Len()).To.Equal(3)
}

func (_ DatabaseTests) QueriesIds() {
	db := createDB()
	defer db.Close()
	result, _ := db.QueryIds("8r", "1r", "4r", "7r").Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 7, 0, 3, 6)
}

func (_ DatabaseTests) QueryIdsNull() {
	db := createDB()
	defer db.Close()
	result, _ := db.QueryIds("8r", "293r").Execute()
	ids := result.Ids()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(2)
	Expect(ids[0]).To.Eql(7)
	Expect(ids[1]).To.Eql(0)
}

func (_ DatabaseTests) QueryIdsDoesntOverflow() {
	QueryPoolSize = 1
	defer func() { QueryPoolSize = 10 }()

	db := createDB()
	defer db.Close()
	result, _ := db.QueryIds("8r", "7r").Execute()
	result.Release()

	result, _ = db.QueryIds("8r", "23r").Execute()
	ids := result.Ids()
	Expect(result.Len()).To.Equal(2)
	Expect(ids[0]).To.Eql(7)
	Expect(ids[1]).To.Eql(0)
}

func (_ DatabaseTests) Each(t func()) {
	sql, _ := newSqliteStorage("test.db")
	_, err := sql.Exec("delete from indexes where id like 'late_%'; delete from updated;")
	if err != nil {
		panic(err)
	}
	sql.Close()
	t()
}

func fakeNewIndexes(db *Database) {
	sql := db.storage.(*SqliteStorage)
	_, err := sql.Exec("insert into updated (id, type) values ('late_set', 2), ('late_list', 3)")
	if err != nil {
		panic(err)
	}

	set := []byte{1, 0, 0, 0, 4, 0, 0, 0, 10, 0, 0, 0}
	list := []byte{8, 0, 0, 0, 10, 0, 0, 0}
	_, err = sql.Exec("insert into indexes (id, payload, type) values ('late_set', ?, 2), ('late_list', ?, 3)", set, list)
	if err != nil {
		panic(err)
	}
}
