package garbage5

import (
	. "github.com/karlseguin/expect"
	"testing"
	"time"
)

type ResultTests struct{}

func Test_Result(t *testing.T) {
	Expectify(new(ResultTests), t)
}

func (_ ResultTests) PoolBlocksWhenDrained() {
	pool := NewResultPool(0, 2)
	a := pool.Checkout()
	pool.Checkout()
	checked := false
	go func() {
		pool.Checkout()
		checked = true
	}()
	time.Sleep(time.Millisecond * 50)
	Expect(checked).To.Equal(false)
	a.Release()
	Expect(pool.Checkout()).Not.To.Equal(nil)
}

func (_ ResultTests) AddIds() {
	result := NewResultPool(10, 1).Checkout()
	result.Add(43, nil)
	result.Add(94, nil)
	result.Add(234, nil)
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
