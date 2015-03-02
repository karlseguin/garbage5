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
	ids := NewIdMap()
	Expect(ids.Internal("a", true)).To.Equal(uint32(1), true)
	Expect(ids.Internal("b", true)).To.Equal(uint32(2), true)
	Expect(ids.Internal("c", true)).To.Equal(uint32(3), true)
}

func (_ IdMapTests) LooksUpExistingIds() {
	ids := NewIdMap()
	ids.Internal("a", true)
	ids.Internal("b", true)
	ids.Internal("c", true)

	Expect(ids.Internal("a", false)).To.Equal(uint32(1), false)
	Expect(ids.Internal("a", true)).To.Equal(uint32(1), false)
	Expect(ids.Internal("c", false)).To.Equal(uint32(3), false)
	Expect(ids.Internal("c", true)).To.Equal(uint32(3), false)
}

func (_ IdMapTests) ReversesIdLookup() {
	ids := NewIdMap()
	ids.Internal("a", true)
	ids.Internal("b", true)
	ids.Internal("c", true)

	Expect(ids.External(1)).To.Equal("a")
	Expect(ids.External(2)).To.Equal("b")
	Expect(ids.External(3)).To.Equal("c")
	Expect(ids.External(4)).To.Equal("")
}

func (_ IdMapTests) EncodesIds() {
	ids := NewIdMap()
	encoded := ids.Encode(449)
	Expect(encoded).To.Equal([]byte{193, 1, 0, 0})
}
