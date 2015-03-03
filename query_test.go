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
	db.CreateSet("set-1", "2r", "3r", "4r", "5r", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
	db.CreateSet("set-2", "3r", "4r", "5r", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
	db.CreateSet("set-3", "4r", "5r", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
	db.CreateSet("set-4", "5r", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
	db.CreateSet("set-5", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
	db.CreateSet("set-6", "1r")
	for i := 1; i < 30; i++ {
		id := strconv.Itoa(i) + "r"
		db.PutResource(FakeResource{id, id})
	}
	Expectify(&QueryTests{db}, t)
}

func (qt QueryTests) EmptyForInvalidSort() {
	result := qt.db.Query("invalid").Execute()
	Expect(result.HasMore()).To.Equal(false)
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
	result := qt.db.Query("recent").Limit(15).Execute()
	Expect(result.HasMore()).To.Equal(false)
}

func (qt QueryTests) DescendingResults() {
	result := qt.db.Query("recent").Desc().Offset(2).Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "13r", "12r")
}

func (qt QueryTests) OneSet() {
	result := qt.db.Query("recent").And("set-1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "2r", "3r")
}

func (qt QueryTests) TwoSets() {
	result := qt.db.Query("recent").And("set-1").And("set-2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) ThreeSets() {
	result := qt.db.Query("recent").And("set-1").And("set-2").And("set-3").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "4r", "5r")
}

func (qt QueryTests) FourSets() {
	result := qt.db.Query("recent").And("set-1").And("set-2").And("set-3").And("set-4").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "5r", "6r")
}

func (qt QueryTests) FiveSets() {
	result := qt.db.Query("recent").And("set-1").And("set-2").And("set-3").And("set-4").And("set-5").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "6r", "7r")
}

func (qt QueryTests) Empty() {
	result := qt.db.Query("recent").And("set-0").Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) Small() {
	result := qt.db.Query("recent").And("set-6").Execute()
	Expect(result.HasMore()).To.Equal(false)
	qt.assertResult(result, "1r")
}

func (qt QueryTests) ZeroLimit() {
	result := qt.db.Query("recent").Limit(0).Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) assertResult(result Result, expected ...string) {
	defer result.Release()
	Expect(result.Len()).To.Equal(len(expected))
	for i, resource := range expected {
		Expect(string(result.Resources()[i])).To.Equal(resource)
	}
}
