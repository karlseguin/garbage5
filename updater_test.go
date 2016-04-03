package indexes

import (
	"io/ioutil"
	"testing"

	. "github.com/karlseguin/expect"
)

type UpdaterTests struct {
	original []byte //store the contents of test.db so we can restore it
}

func Test_Updater(t *testing.T) {
	original, err := ioutil.ReadFile("test.db")
	if err != nil {
		panic(err)
	}
	Expectify(&UpdaterTests{original}, t)
}

func (u UpdaterTests) Each(test func()) {
	defer func() {
		ioutil.WriteFile("test.db", u.original, 0644)
	}()
	test()
}

func (_ UpdaterTests) UpdatesASet() {
	db := createDB()
	defer db.Close()

	updater := db.Update()
	updater.SetUpdate("7", 10)
	updater.SetUpdate("7", 6)
	updater.SetDelete("7", 2)
	updater.Commit()

	set := db.GetSet("7")
	Expect(set.Len()).To.Equal(4)
	Expect(set.Exists(7)).To.Equal(true)
	Expect(set.Exists(10)).To.Equal(true)
	Expect(set.Exists(6)).To.Equal(true)
	Expect(set.Exists(5)).To.Equal(true)
}

func (_ UpdaterTests) UpdatesAList() {
	db := createDB()
	defer db.Close()

	updater := db.Update()
	updater.ListUpdate("recent", 20, 2)
	updater.ListUpdate("recent", 19, 3)
	updater.ListUpdate("recent", 23, 6)
	updater.ListDelete("recent", 11)
	updater.ListDelete("recent", 13)
	updater.Commit()

	index := 0
	expected := []Id{1, 2, 20, 19, 3, 4, 23, 5, 6, 7, 8, 9, 10, 12, 14, 15}
	db.GetList("recent").Each(false, func(id Id) bool {
		Expect(id).ToEqual(expected[index])
		index++
		return true
	})
}

// map[5r:4 7r:6 13r:12 3r:2 8r:7 2r:1 1r:0 4r:3 9r:8 14r:13 12r:11 6r:5 15r:14 10r:9 11r:10]
func (_ UpdaterTests) UpdatesIds() {
	db := createDB()
	defer db.Close()

	size := len(db.ids)

	updater := db.Update()
	updater.IdsUpdate("x", 19)
	updater.IdsUpdate("y", 20)
	updater.IdsDelete("5r")
	updater.IdsDelete("7r")
	updater.IdsDelete("13r")
	updater.Commit()

	Expect(len(db.ids)).To.Equal(size - 1)
	Expect(db.ids["x"]).To.Equal(Id(19))
	Expect(db.ids["y"]).To.Equal(Id(20))
	Expect(db.ids["5r"]).To.Equal(Id(0))
	Expect(db.ids["7r"]).To.Equal(Id(0))
	Expect(db.ids["13r"]).To.Equal(Id(0))
	Expect(db.ids["3r"]).To.Equal(Id(2))
}
