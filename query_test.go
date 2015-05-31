package indexes

import (
	. "github.com/karlseguin/expect"
	"testing"
)

type QueryTests struct {
	db *Database
}

func Test_Query(t *testing.T) {
	Expectify(&QueryTests{createDB()}, t)
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
	result := qt.db.Query("recent").And("1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "2r", "3r")
}

func (qt QueryTests) TwoSets() {
	result := qt.db.Query("recent").And("1").And("2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) ThreeSets() {
	result := qt.db.Query("recent").And("1").And("2").And("3").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "4r", "5r")
}

func (qt QueryTests) FourSets() {
	result := qt.db.Query("recent").And("1").And("2").And("3").And("4").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "5r", "6r")
}

func (qt QueryTests) FiveSets() {
	result := qt.db.Query("recent").And("1").And("2").And("3").And("4").And("5").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "6r", "7r")
}

func (qt QueryTests) OneSetBasedFind() {
	result := qt.db.Query("large").And("1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "2r", "3r")
}

func (qt QueryTests) TwoSetBasedFind() {
	result := qt.db.Query("large").And("1").And("2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) SetBasedNoMore() {
	result := qt.db.Query("large").And("1").And("2").Limit(2).Offset(11).Execute()
	Expect(result.HasMore()).To.Equal(false)
	qt.assertResult(result, "14r", "15r")
}

func (qt QueryTests) SetBasedDesc() {
	result := qt.db.Query("large").And("1").And("2").Limit(2).Offset(1).Desc().Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "14r", "13r")
}

func (qt QueryTests) SetBasedDescNoMore() {
	result := qt.db.Query("large").And("1").And("2").Limit(2).Offset(11).Desc().Execute()
	Expect(result.HasMore()).To.Equal(false)
	qt.assertResult(result, "4r", "3r")
}

func (qt QueryTests) SetBasedOutOfRangeOffset() {
	result := qt.db.Query("large").And("1").And("2").Limit(2).Desc().Offset(14).Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) Empty() {
	result := qt.db.Query("recent").And("0").Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) Small() {
	result := qt.db.Query("recent").And("6").Execute()
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
		id := result.Ids()[i]
		Expect(qt.db.ids[id]).To.Equal(resource)
	}
}

func createDB() *Database {
	db, err := New(Configure().Path("./test.db"))
	if err != nil {
		panic(err)
	}
	return db
}
