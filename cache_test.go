package indexes

import (
	"testing"
	"time"

	. "github.com/karlseguin/expect"
)

type CacheTest struct {
}

func Test_Cache(t *testing.T) {
	Expectify(&CacheTest{}, t)
}

func (_ CacheTest) FetchesItems() {
	_, result := buildCache(1024, time.Second*10)
	result.add(1)
	result.add(2)
	result.add(4)
	result.fill(false)

	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(3)
	Expect(string(payloads[0])).To.Eql(`{"id": "1r"}`)
	Expect(string(payloads[1])).To.Eql(`{"id": "2r"}`)
	Expect(string(payloads[2])).To.Eql(`{"id": "4r"}`)
}

func (_ CacheTest) FetchesDetailedItems() {
	_, result := buildCache(1024, time.Second*10)
	result.add(1)
	result.add(2)
	result.add(4)
	result.fill(true)

	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(3)
	Expect(string(payloads[0])).To.Eql(`{"id": "1rd"}`)
	Expect(string(payloads[1])).To.Eql(`{"id": "2rd"}`)
	Expect(string(payloads[2])).To.Eql(`{"id": "4rd"}`)
}

func (_ CacheTest) GetsItemsFromCache() {
	cache, result := buildCache(1024, time.Second*10)
	cache.fetcher = nil
	cache.Set(2, []byte("33"), false)
	cache.Set(4, []byte("44"), false)
	result.add(2)
	result.add(4)
	result.fill(false)
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("33")
	Expect(payloads[1]).To.Eql("44")
}

func (_ CacheTest) MixesCachedAndUncachedResults() {
	cache, result := buildCache(1024, time.Second*10)
	cache.Set(2, []byte("234"), false)
	result.add(2)
	result.add(10)
	result.fill(false)
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("234")
	Expect(payloads[1]).To.Eql(`{"id": "10r"}`)

	Expect(cache.bucket(10, false).get(10).value).To.Eql(`{"id": "10r"}`)
}

func (_ CacheTest) DoesntReturnExpiredItem() {
	cache, result := buildCache(1024, time.Second*-10)
	cache.Set(2, []byte("234"), false)
	result.add(2)
	result.add(9)
	result.fill(false)
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql(`{"id": "2r"}`)
	Expect(payloads[1]).To.Eql(`{"id": "9r"}`)
}

func (_ CacheTest) Fetch() {
	cache, _ := buildCache(1024, time.Second*10)
	Expect(cache.Fetch(2)).To.Eql(`{"id": "2rd"}`)
	Expect(cache.Fetch(2)).To.Eql(`{"id": "2rd"}`)
}

func (_ CacheTest) FetchFallsBackToSummary() {
	cache, _ := buildCache(1024, time.Second*10)
	Expect(cache.Fetch(9999999)).To.Eql(`{"id": "9999999x"}`)
	Expect(cache.Fetch(9999999)).To.Eql(`{"id": "9999999x"}`)
}

func buildCache(size uint64, ttl time.Duration) (*Cache, *NormalResult) {
	storage, err := newSqliteStorage("./test.db")
	if err != nil {
		panic(err)
	}
	cache, err := newCache(storage, Configure().CacheSize(size).CacheTTL(ttl))
	if err != nil {
		panic(err)
	}
	result := newResult(cache, 10, 10)
	return cache, result
}
