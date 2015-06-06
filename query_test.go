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
	qt.assertResult(result, "1r", "2r", "3r")
}

func (qt QueryTests) AppliesAnOffset() {
	result, _ := qt.db.Query().Sort("recent").Offset(2).Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) HasNoMore() {
	result, _ := qt.db.Query().Sort("recent").Limit(15).Execute()
	Expect(result.HasMore()).To.Equal(false)
}

func (qt QueryTests) DescendingResults() {
	result, _ := qt.db.Query().Sort("recent").Desc().Offset(2).Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "13r", "12r")
}

func (qt QueryTests) UsesAListAsASet() {
	result, _ := qt.db.Query().Sort("large").And("recent").Execute()
	Expect(result.HasMore()).To.Equal(false)
	qt.assertResult(result, "1r", "2r", "3r", "4r", "5r", "6r", "7r", "8r", "9r", "10r", "11r", "12r", "13r", "14r", "15r")
}

func (qt QueryTests) OneSet() {
	result, _ := qt.db.Query().Sort("recent").And("1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "2r", "3r")
}

func (qt QueryTests) TwoSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) ThreeSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").And("3").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "4r", "5r")
}

func (qt QueryTests) FourSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").And("3").And("4").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "5r", "6r")
}

func (qt QueryTests) FiveSets() {
	result, _ := qt.db.Query().Sort("recent").And("1").And("2").And("3").And("4").And("5").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "6r", "7r")
}

func (qt QueryTests) OneSetBasedFind() {
	result, _ := qt.db.Query().Sort("large").And("1").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "2r", "3r")
}

func (qt QueryTests) TwoSetBasedFind() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "3r", "4r")
}

func (qt QueryTests) SetBasedNoMore() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Offset(11).Execute()
	Expect(result.HasMore()).To.Equal(false)
	qt.assertResult(result, "14r", "15r")
}

func (qt QueryTests) SetBasedDesc() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Offset(1).Desc().Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, "14r", "13r")
}

func (qt QueryTests) SetBasedDescNoMore() {
	result, _ := qt.db.Query().Sort("large").And("1").And("2").Limit(2).Offset(11).Desc().Execute()
	Expect(result.HasMore()).To.Equal(false)
	qt.assertResult(result, "4r", "3r")
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
	qt.assertResult(result, "1r")
}

func (qt QueryTests) ZeroLimit() {
	result, _ := qt.db.Query().Sort("recent").Limit(0).Execute()
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
