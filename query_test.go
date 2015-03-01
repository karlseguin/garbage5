package garbage5

import (
	. "github.com/karlseguin/expect"
	"testing"
)

type QueryTests struct {
	*Database
}

func Test_Query(t *testing.T) {
	// all tests share the same DB instance since they only read from it.
	// alternatively, make it possiblet o create a purely in-memory database
	// so that we can cheaply create them without any I/O
	db := createDB()
	db.CreateList("recent", "0s", "1s", "2s", "3s", "4s", "5s", "6s", "7s", "8s", "9s", "As", "Bs", "Cs", "Ds", "Es", "Fs")
	Expectify(&QueryTests{db}, t)
}

func (qt QueryTests) ErrorOnInvalidSort() {
	result, err := qt.Query("invalid").Execute()
	Expect(err).To.Equal(InvalidSortErr)
	Expect(result).To.Equal(nil)
}

func (qt QueryTests) LimitsNumberOfResults() {
	result, err := qt.Query("recent").Limit(3).Execute()
	Expect(result.HasMore()).To.Equal(true)
	qt.assertResult(result, err, "0s", "1s", "2s")
}

func (qt QueryTests) HasNoMore() {
	result, _ := qt.Query("recent").Limit(20).Execute()
	Expect(result.HasMore()).To.Equal(false)
	Expect(result.Len()).To.Equal(16)
}

func (qt QueryTests) assertResult(result Result, err error, expected ...string) {
	defer result.Release()
	Expect(err).To.Equal(nil)
	Expect(result.Len()).To.Equal(len(expected))
	for i, id := range expected {
		Expect(result.Ids()[i]).To.Equal(qt.ids.Internal(id, false))
	}
}
