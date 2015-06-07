package indexes

import (
	"os"
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
	defer db.Close()
	res, _ := db.Query().Sort("late").Execute()
	Expect(res.Len()).To.Equal(0)

	addList(db, "late_list", 8, 10)

	db.Update()
	res, _ = db.Query().Sort("late_list").Execute()
	Expect(res.Len()).To.Equal(2)
}

func (_ DatabaseTests) UpdateLoadsANewSet() {
	db := createDB()
	defer db.Close()
	// res, _ := db.Query().And("late").Execute()
	// Expect(res.Len()).To.Equal(0)

	addSet(db, "late_set", 1, 4, 10)

	db.Update()
	res, _ := db.Query().And("late_set").Execute()
	Expect(res.Len()).To.Equal(3)
}

func (_ DatabaseTests) Each(test func()) {
	sql, _ := newSqliteStorage("test.db")
	defer sql.Close()
	_, err := sql.Exec("drop table if exists list_late_list; drop table if exists set_late_set;drop table if exists updated;")
	if err != nil {
		panic(err)
	}
	test()
}

func addList(db *Database, name string, ids ...int) {
	if os.Getenv("DB_TYPE") == "redis" {
		addRedisList(db, name, ids...)
	} else {
		addSqliteList(db, "list_"+name, ids...)
	}
}

func addSet(db *Database, name string, ids ...int) {
	if os.Getenv("DB_TYPE") == "redis" {
		addRedisSet(db, name, ids...)
	} else {
		addSqliteList(db, "set_"+name, ids...)
	}
}

func addRedisList(db *Database, name string, ids ...int) {
	conn := db.storage.(*RedisStorage).Get()
	defer conn.Close()
	conn.Do("del", "updated_lists")
	conn.Do("sadd", "updated_lists", "list_late_list")
	for index, id := range ids {
		if _, err := conn.Do("zadd", "list_late_list", index, id); err != nil {
			panic(err)
		}
	}
}

func addRedisSet(db *Database, name string, ids ...int) {
	conn := db.storage.(*RedisStorage).Get()
	defer conn.Close()
	conn.Do("del", "updated_sets")
	conn.Do("sadd", "updated_sets", "set_late_set")
	for _, id := range ids {
		if _, err := conn.Do("sadd", "set_late_set", id); err != nil {
			panic(err)
		}
	}
}

func addSqliteList(db *Database, name string, ids ...int) {
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
