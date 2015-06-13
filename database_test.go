package indexes

import (
	"testing"

	. "github.com/karlseguin/expect"
)

type DatabaseTests struct {
}

func Test_Database(t *testing.T) {
	// Expectify(&DatabaseTests{}, t)
}

func (_ DatabaseTests) UpdateLoadsANewList() {
	db := createDB()
	defer db.Close()
	res, _ := db.Query().Sort("late_list").Execute()
	Expect(res.Len()).To.Equal(0)

	fakeNewIndexes(db)
	db.Update()
	res, _ = db.Query().Sort("late_list").Execute()
	Expect(res.Len()).To.Equal(2)
}

func (_ DatabaseTests) UpdateLoadsANewSet() {
	db := createDB()
	defer db.Close()
	res, _ := db.Query().And("late_set").Execute()
	Expect(res.Len()).To.Equal(0)

	fakeNewIndexes(db)
	db.Update()
	res, _ = db.Query().And("late_set").Execute()
	Expect(res.Len()).To.Equal(3)
}

func (_ DatabaseTests) Each(t func()) {
	sql, _ := newSqliteStorage("test.db")
	defer sql.Close()
	_, err := sql.Exec("delete from names where name like 'late_%'")
	if err != nil {
		panic(err)
	}
}

func cleanNewIndexes(db *Database) {
	sql := db.storage.(*SqliteStorage)
	_, err := sql.Exec("delete from names where name like 'late_%'")
	if err != nil {
		panic(err)
	}
}

func fakeNewIndexes(db *Database) {
	sql := db.storage.(*SqliteStorage)
	_, err := sql.Exec("insert into names (id, name, type) values (3, 'late_set', 1), (4, 'late_list', 2)")
	if err != nil {
		panic(err)
	}
}
