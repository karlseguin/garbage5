package garbage5

import (
	. "github.com/karlseguin/expect"
	"testing"
)

type ResultTests struct{}

func Test_Result(t *testing.T) {
	Expectify(new(ResultTests), t)
}

func (_ ResultTests) AddIds() {
	db := createDB()
	result := db.Query("aa").result

	result.add(43)
	result.add(94)
	result.add(234)
	Expect(result.Len()).To.Equal(3)
	Expect(result.Ids()).To.Equal([]uint32{43, 94, 234})

	result.Release()
	Expect(result.Len()).To.Equal(0)
	Expect(result.Ids()).To.Equal([]uint32{})
}

type FakeResource struct {
	id   string
	body string
}

func (r FakeResource) Id() string {
	return r.id
}

func (r FakeResource) Bytes() []byte {
	return []byte(r.body)
}
