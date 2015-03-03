package cache

import (
	. "github.com/karlseguin/expect"
	"strconv"
	"testing"
	"time"
)

type CacheTests struct{}

func Test_Cache(t *testing.T) {
	Expectify(new(CacheTests), t)
}

func (_ CacheTests) SetsAValue() {
	cache := New(100000)
	cache.Set(1, []byte("the spice"))
	cache.Set(2, []byte("must flow"))
	Expect(string(cache.Get(1))).To.Equal("the spice")
	Expect(string(cache.Get(2))).To.Equal("must flow")
}

func (_ CacheTests) SetsOfNewItemAdjustsSize() {
	cache := New(100000)
	cache.Set(44, []byte("12345"))
	time.Sleep(time.Millisecond * 10)
	Expect(cache.size).To.Equal(5)
}

func (_ CacheTests) SetOfReplacementAdjustsSize() {
	cache := New(100000)
	cache.Set(44, []byte("12345"))
	time.Sleep(time.Millisecond * 10)
	cache.Set(44, []byte("123"))
	time.Sleep(time.Millisecond * 10)
	Expect(cache.size).To.Equal(3)
}

func (_ CacheTests) GetsNil() {
	cache := New(100000)
	Expect(cache.Get(555)).To.Equal(nil)
}

func (_ CacheTests) GCsTheOldestItems() {
	cache := New(10000)
	for i := 0; i < 1500; i++ {
		cache.Set(uint32(i), []byte(strconv.Itoa(i)))
	}
	time.Sleep(time.Millisecond * 10)
	Expect(cache.size).To.Equal(4890)
	cache.gc()
	Expect(cache.Get(5)).To.Equal(nil)
	Expect(cache.Get(999)).To.Equal(nil)
	Expect(string(cache.Get(1000))).To.Equal("1000")
	Expect(cache.size).To.Equal(2000)
}

func (_ CacheTests) GetPromotesAValue() {
	cache := New(10000)
	for i := 0; i < 1500; i++ {
		cache.Set(uint32(i), []byte(strconv.Itoa(i)))
	}
	// trigger the protomotion
	cache.Get(1)
	cache.Get(1)
	cache.Get(1)
	cache.Get(1)
	cache.Get(1)
	time.Sleep(time.Millisecond * 10)
	cache.gc()
	Expect(cache.Get(999)).To.Equal(nil)
	Expect(cache.Get(1000)).To.Equal(nil)
	Expect(string(cache.Get(1))).To.Equal("1")
	Expect(string(cache.Get(1001))).To.Equal("1001")
	Expect(cache.size).To.Equal(1997)
}
