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

// '1r', '2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'
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
