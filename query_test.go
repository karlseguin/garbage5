package garbage5

import (
	. "github.com/karlseguin/expect"
	"strconv"
	"testing"
)

type QueryTests struct {
	db *Database
}

func Test_Query(t *testing.T) {
	// all tests share the same DB instance since they only read from it.
	// alternatively, make it possible to create a purely in-memory database
	// so that we can cheaply create them without any I/O
	db := createDB()
	db.CreateList("recent", "0r", "1r", "2r", "3r", "4r", "5r", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
	for i := 1; i < 30; i++ {
		id := strconv.Itoa(i) + "r"
		db.PutResource(FakeResource{id, id})
	}
	Expectify(&QueryTests{db}, t)
}

func (qt QueryTests) ErrorOnInvalidSort() {
	result, err := qt.db.Query("invalid").Execute()
	Expect(err).To.Equal(InvalidSortErr)
	Expect(result).To.Equal(nil)
}

func (qt QueryTests) LimitsNumberOfResults() {
	result, err := qt.db.Query("recent").Limit(3).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, err, "1r", "2r", "3r")
}

func (qt QueryTests) HasNoMore() {
	result, _ := qt.db.Query("recent").Limit(20).Execute()
	Expect(result.HasMore()).To.Equal(false)
}

func (qt QueryTests) assertResult(result Result, err error, expected ...string) {
	defer result.Release()
	Expect(err).To.Equal(nil)
	Expect(result.Len()).To.Equal(len(expected))
	for i, resource := range expected {
		Expect(string(result.Resources()[i])).To.Equal(resource)
	}
}
