package cache

import (
	. "github.com/karlseguin/expect"
	"testing"
)

type BucketTests struct{}

func Test_Bucket(t *testing.T) {
	Expectify(new(BucketTests), t)
}

func (_ BucketTests) GetMiss() {
	bucket := testBucket()
	Expect(bucket.get(55)).To.Equal(nil)
}

func (_ BucketTests) GetHit() {
	bucket := testBucket()
	Expect(string(bucket.get(9001).data)).To.Equal("goku")
}

func (_ BucketTests) Delete() {
	bucket := testBucket()
	bucket.delete(9001)
	Expect(bucket.get(9001)).To.Equal(nil)
}

func (_ BucketTests) SetsANewBucketItem() {
	bucket := testBucket()
	entry := &Entry{id: 1234, data: []byte("vegeta")}
	Expect(bucket.set(1234, entry)).To.Equal(nil)
	Expect(string(bucket.get(1234).data)).To.Equal("vegeta")
}

func (_ BucketTests) SetsAnExistingItem() {
	bucket := testBucket()
	entry := &Entry{id: 4321, data: []byte("vegeta")}
	bucket.set(4321, entry)
	existing := bucket.set(4321, &Entry{id: 4321, data: []byte("vegeta2")})
	Expect(string(existing.data)).To.Equal("vegeta")
	Expect(string(bucket.get(4321).data)).To.Equal("vegeta2")
}

func testBucket() *bucket {
	b := &bucket{lookup: make(map[uint32]*Entry)}
	b.lookup[9001] = &Entry{id: 9001, data: []byte("goku")}
	b.lookup[700] = &Entry{id: 700, data: []byte("gohan")}
	return b
}
