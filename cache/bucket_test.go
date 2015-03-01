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

//
// func (_ BucketTests) Delete() {
// 	bucket := testBucket()
// 	bucket.delete("power", "level")
// 	Expect(bucket.get("power", "level")).To.Equal(nil)
// 	// assertEntry(bucket, "power", "rating", "high")
// }
//
// func (_ BucketTests) DeleteAll() {
// 	bucket := testBucket()
// 	bucket.deleteAll("power")
// 	Expect(bucket.get("power", "level")).To.Equal(nil)
// 	Expect(bucket.get("power", "rating")).To.Equal(nil)
// }
//
// func (_ BucketTests) SetsANewBucketItem() {
// 	bucket := testBucket()
// 	entry := buildEntry("flow")
// 	Expect(bucket.set("spice", "must", entry)).To.Equal(nil)
// 	assertEntry(bucket, "power", "level", "over 9000!")
// 	assertEntry(bucket, "spice", "must", "flow")
// }
//
// func (_ BucketTests) SetsAnExistingItem() {
// 	bucket := testBucket()
// 	entry := buildEntry("9002")
// 	existing := bucket.set("power", "level", entry)
// 	assertResponse(existing, "over 9000!")
// 	assertEntry(bucket, "power", "level", "9002")
// }

func testBucket() *bucket {
	b := &bucket{lookup: make(map[uint32]*Entry)}
	b.lookup[9001] = &Entry{id: 9001, data: []byte("goku")}
	b.lookup[700] = &Entry{id: 700, data: []byte("gohan")}
	return b
}
