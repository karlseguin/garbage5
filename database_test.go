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
