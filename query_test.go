package indexes

import (
	"testing"

	. "github.com/karlseguin/expect"
)

type QueryTests struct {
	db *Database
}

func Test_Query(t *testing.T) {
	Expectify(&QueryTests{createDB()}, t)
}

func (qt QueryTests) EmptyForInvalidSort() {
	result, _ := qt.db.Query().Sort("invalid").Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) EmptyForNoSort() {
	result, _ := qt.db.Query().Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) HandlesOnlyHavingOneSet() {
	result, _ := qt.db.Query().And("1").Limit(3).Execute()
	Expect(result.HasMore()).To.Equal(true)
	Expect(result.Len()).To.Equal(3)
}

func (qt QueryTests) LimitsNumberOfResults() {
	result, _ := qt.db.Query().Sort("recent").Limit(3).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 1, 2, 3)
}

func (qt QueryTests) ReturnsResultsAroundAnother() {
	result, _ := qt.db.Query().Sort("recent").Around(7).Limit(5).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 8, 6, 9, 5, 10)
}

func (qt QueryTests) ReturnsResultsAroundAtTheHead() {
	result, _ := qt.db.Query().Sort("recent").Around(2).Limit(5).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 3, 1, 4, 5, 6)
}

func (qt QueryTests) ReturnsResultsAroundAtTheTail() {
	result, _ := qt.db.Query().Sort("recent").Around(14).Limit(5).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 15, 13, 12, 11, 10)
}

func (qt QueryTests) ReturnsResultsAroundFiltered() {
	result, _ := qt.db.Query().Sort("recent").And("7").Around(7).Limit(5).Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 5, 10, 2)
}

func (qt QueryTests) RetrievesResultSummary() {
	result, _ := qt.db.Query().Sort("recent").Limit(2).Execute()
	payloads := result.Payloads()
	Expect(payloads[0]).To.Equal(JSON(`{"id": "1r"}`))
	Expect(payloads[1]).To.Equal(JSON(`{"id": "2r"}`))
}

func (qt QueryTests) RetrievesResultDetails() {
	result, _ := qt.db.Query().Detailed().Sort("recent").Limit(2).Execute()
	payloads := result.Payloads()
	Expect(payloads[0]).To.Equal(JSON(`{"id": "1rd"}`))
	Expect(payloads[1]).To.Equal(JSON(`{"id": "2rd"}`))
}

func (qt QueryTests) AppliesAnOffset() {
	result, err := qt.db.Query().Sort("recent").Offset(2).Limit(2).Execute()
	if err != nil {
		panic(err)
	}
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 3, 4)
}

func (qt QueryTests) HasNoMore() {
	result, _ := qt.db.Query().Sort("recent").Limit(15).Execute()
	Expect(result.HasMore()).To.Equal(false)
}

func (qt QueryTests) DescendingResults() {
	result, _ := qt.db.Query().Sort("recent").Desc().Offset(2).Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 13, 12)
}

func (qt QueryTests) UsesAListAsASet() {
	result, _ := qt.db.Query().Sort("large").And("recent").Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
}

func (qt QueryTests) OneSet() {
	result, _ := qt.db.Query().Sort("recent").And("1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 2, 3)
}

func (qt QueryTests) TwoSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 3, 4)
}

func (qt QueryTests) ThreeSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").And("3").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 4, 5)
}

func (qt QueryTests) FourSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").And("3").And("4").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 5, 6)
}

func (qt QueryTests) FiveSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").And("3").And("4").And("5").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 6, 7)
}

func (qt QueryTests) OneSetBasedFind() {
	result, _ := qt.db.Query().Sort("large").And("1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 2, 3)
}

func (qt QueryTests) TwoSetBasedFind() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 3, 4)
}

func (qt QueryTests) OneListOneSet() {
	result, _ := qt.db.Query().Sort("large").And("other").And("1").Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 10, 12)
}

func (qt QueryTests) SetBasedNoMore() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Offset(11).Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 14, 15)
}

func (qt QueryTests) SetBasedDesc() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Offset(1).Desc().Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 14, 13)
}

func (qt QueryTests) SetBasedAround() {
	result, _ := qt.db.Query().Around(8).Sort("large").And("1").And("2").Limit(5).Execute()
	Expect(result.HasMore()).To.Equal(true)
	assertResult(result, 9, 7, 10, 6, 11)
}

func (qt QueryTests) SetBasedDescNoMore() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Offset(11).Desc().Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 4, 3)
}

func (qt QueryTests) SetBasedOutOfRangeOffset() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Desc().Offset(14).Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) Empty() {
	result, _ := qt.db.Query().Sort("recent").And("0").Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func (qt QueryTests) Small() {
	result, _ := qt.db.Query().Sort("recent").And("6").Execute()
	Expect(result.HasMore()).To.Equal(false)
	assertResult(result, 1)
}

func (qt QueryTests) ZeroLimit() {
	result, _ := qt.db.Query().Sort("recent").Limit(0).Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(0)
}

func assertResult(result Result, expected ...uint32) {
	defer result.Release()
	Expect(result.Len()).To.Equal(len(expected))
	for i, resource := range expected {
		id := result.Ids()[i]
		Expect(id).To.Equal(resource)
	}
}

func createDB() *Database {
	c := Configure().Path("./test.db")
	db, err := New(c)
	if err != nil {
		panic(err)
	}
	return db
}
