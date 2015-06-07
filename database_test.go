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

func (_ DatabaseTests) UpdateLoadsANewList() {
	db := createDB()
	res, _ := db.Query().Sort("late").Execute()
	Expect(res.Len()).To.Equal(0)

	addList("list_late", 8, 10)

	db.Update()
	res, _ = db.Query().Sort("late").Execute()
	Expect(res.Len()).To.Equal(2)
}

func (_ DatabaseTests) UpdateLoadsANewSet() {
	db := createDB()
	res, _ := db.Query().And("late").Execute()
	Expect(res.Len()).To.Equal(0)

	addList("set_late", 1, 4, 10)

	db.Update()
	res, _ = db.Query().And("late").Execute()
	Expect(res.Len()).To.Equal(3)
}

func (_ DatabaseTests) Each(test func()) {
	db := createDB()
	db.storage.ClearNew()
	sql := db.storage.(*SqliteStorage)
	sql.Exec("drop table list_late; drop table set_late")
	db.Close()
	test()
}

func addList(name string, ids ...int) {
	db := createDB()
	sql := db.storage.(*SqliteStorage)
	sql.Exec("create table " + name + " (id, sort); create table updated (name)")
	if _, err := sql.Exec("insert into updated (name) values (?)", name); err != nil {
		panic(err)
	}
	for index, id := range ids {
		if _, err := sql.Exec("insert into "+name+"(id, sort) values (?, ?)", id, index); err != nil {
			panic(err)
		}
	}
}
