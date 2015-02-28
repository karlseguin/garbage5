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
	db.CreateList("recent", "1s", "2s", "3s", "4s", "5s", "6s", "7s", "8s", "9s", "As", "Bs", "Cs", "Ds", "Es", "Fs")
	Expectify(&QueryTests{db}, t)
}

func (qt QueryTests) LimitsNumberOfResults() {
	result := qt.Query("recent").Limit(3).Execute()
	assertResult(result, qt.Id("1s"), qt.Id("2s"), qt.Id("3s"))
}

func assertResult(result Result, expected ...uint32) {
	defer result.Release()
	Expect(result.Len()).To.Equal(len(expected))
	for i, id := range expected {
		Expect(result.Ids()[i]).To.Equal(id)
	}
}
