package garbage5

import (
	. "github.com/karlseguin/expect"
	"os"
	"testing"
)

const TMP_PATH = "/tmp/garbage5.db"

type DatabaseTests struct{}

func Test_Database(t *testing.T) {
	Expectify(new(DatabaseTests), t)
}

func (_ DatabaseTests) CreatesAList() {
	db := createDB()
	db.CreateList("test:list", "a-1", "b-2", "c-3")
	assertList(db, "test:list", "a-1", "b-2", "c-3")
	db.Close()
	assertList(openDB(), "test:list", "a-1", "b-2", "c-3")
}

func (_ DatabaseTests) CreatesASet() {
	db := createDB()
	db.CreateSet("test:set", "a-1", "b-2", "c-3")
	assertSet(db, "test:set", "a-1", "b-2", "c-3")
	db.Close()
	assertSet(openDB(), "test:set", "a-1", "b-2", "c-3")
}

func createDB() *Database {
	os.Remove(TMP_PATH) //ignore failures
	return openDB()
}

func openDB() *Database {
	db, err := New(Configure().Path(TMP_PATH))
	if err != nil {
		panic(err)
	}
	return db
}

func assertList(db *Database, name string, expected ...string) {
	list := db.List(name)
	Expect(list.Len()).To.Equal(len(expected))
	i := 0

	list.Each(func(id uint32) bool {
		internal, _ := db.ids.Internal(expected[i], false)
		Expect(id).To.Equal(internal)
		i++
		return true
	})
}

func assertSet(db *Database, name string, expected ...string) {
	set := db.Set(name)
	Expect(set.Len()).To.Equal(len(expected))
	for _, id := range expected {
		internal, _ := db.ids.Internal(id, false)
		Expect(set.Exists(internal)).To.Equal(true)
	}
}
