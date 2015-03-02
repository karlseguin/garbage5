package garbage5

import (
	. "github.com/karlseguin/expect"
	"testing"
)

type IdMapTests struct{}

func Test_IdMap(t *testing.T) {
	Expectify(new(IdMapTests), t)
}

func (_ IdMapTests) CreatesNewIds() {
	ids := NewIdMap(NullIdWriter)
	Expect(ids.Internal("a")).To.Equal(uint32(1), []byte{1, 0, 0, 0})
	Expect(ids.Internal("b")).To.Equal(uint32(2), []byte{2, 0, 0, 0})
	Expect(ids.Internal("c")).To.Equal(uint32(3), []byte{3, 0, 0, 0})
}

func (_ IdMapTests) LooksUpExistingIds() {
	ids := NewIdMap(NullIdWriter)
	ids.Internal("a")
	ids.Internal("b")
	ids.Internal("c")

	Expect(ids.Internal("a")).To.Equal(uint32(1), []byte{1, 0, 0, 0})
	Expect(ids.Internal("a")).To.Equal(uint32(1), []byte{1, 0, 0, 0})
	Expect(ids.Internal("c")).To.Equal(uint32(3), []byte{3, 0, 0, 0})
}

func (_ IdMapTests) ReversesIdLookup() {
	ids := NewIdMap(NullIdWriter)
	ids.Internal("a")
	ids.Internal("b")
	ids.Internal("c")

	Expect(ids.External(1)).To.Equal("a")
	Expect(ids.External(2)).To.Equal("b")
	Expect(ids.External(3)).To.Equal("c")
	Expect(ids.External(4)).To.Equal("")
}

func (_ IdMapTests) GetsBytesForId() {
	ids := NewIdMap(NullIdWriter)
	id, _ := ids.Internal("44")
	Expect(ids.Bytes(id)).To.Equal([]byte{1, 0, 0, 0})
}

func NullIdWriter(k, v []byte) {}
