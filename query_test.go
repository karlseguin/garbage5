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

func (qt QueryTests) EmptyForInvalidSort() {
	result := qt.db.Query("invalid").Execute()
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) LimitsNumberOfResults() {
	result := qt.db.Query("recent").Limit(3).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "1r", "2r", "3r")
}

func (qt QueryTests) AppliesAnOffset() {
	result := qt.db.Query("recent").Offset(2).Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) HasNoMore() {
	result := qt.db.Query("recent").Limit(20).Execute()
	Expect(result.HasMore()).To.Equal(false)
}

func (qt QueryTests) assertResult(result Result, expected ...string) {
	defer result.Release()
	Expect(result.Len()).To.Equal(len(expected))
	for i, resource := range expected {
		Expect(string(result.Resources()[i])).To.Equal(resource)
	}
}
