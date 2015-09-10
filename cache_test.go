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
	_, result := buildCache(1024, time.Second*10, 0)
	result.add(1)
	result.add(2)
	result.add(4)
	result.fill(false)

	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(3)
	Expect(payloads[0]).To.Eql(`{"id": "1r"}`)
	Expect(payloads[1]).To.Eql(`{"id": "2r"}`)
	Expect(payloads[2]).To.Eql(`{"id": "4r"}`)
}

func (_ CacheTest) FetchFromFillFollowsType() {
	cache, result := buildCache(1024, time.Second*10, 0)
	result.add(1)
	result.add(2)
	result.add(4)
	result.fill(true)
	cache.fetcher = nil

	Expect(cache.Fetch(1, "user")).To.Eql(`{"id": "1rd"}`)
	Expect(cache.Fetch(1, "page")).To.Equal(nil)
	Expect(cache.Fetch(2, "user")).To.Equal(nil)
	Expect(cache.Fetch(2, "page")).To.Eql(`{"id": "2rd"}`)
	Expect(cache.Fetch(4, "user")).To.Equal(nil)
	Expect(cache.Fetch(4, "page")).To.Eql(`{"id": "4rd"}`)

}

func (_ CacheTest) FetchesDetailedItems() {
	_, result := buildCache(1024, time.Second*10, 0)
	result.add(1)
	result.add(2)
	result.add(4)
	result.fill(true)

	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(3)
	Expect(payloads[0]).To.Eql(`{"id": "1rd"}`)
	Expect(payloads[1]).To.Eql(`{"id": "2rd"}`)
	Expect(payloads[2]).To.Eql(`{"id": "4rd"}`)
}

func (_ CacheTest) GetsItemsFromCache() {
	cache, result := buildCache(1024, time.Second*10, 0)
	cache.fetcher = nil
	cache.set(2, Value{1, []byte("33")}, false)
	cache.set(4, Value{1, []byte("44")}, false)
	result.add(2)
	result.add(4)
	result.fill(false)
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("33")
	Expect(payloads[1]).To.Eql("44")
}

func (_ CacheTest) MixesCachedAndUncachedResults() {
	cache, result := buildCache(1024, time.Second*10, 0)
	cache.set(2, Value{1, []byte("234")}, false)
	result.add(2)
	result.add(10)
	result.fill(false)
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("234")
	Expect(payloads[1]).To.Eql(`{"id": "10r"}`)

	Expect(cache.bucket(10, false).get(10).Value.payload).To.Eql(`{"id": "10r"}`)
}

func (_ CacheTest) DoesntReturnExpiredItem() {
	cache, result := buildCache(1024, time.Second*-10, 0)
	cache.set(2, Value{1, []byte("234")}, false)
	result.add(2)
	result.add(9)
	result.fill(false)
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql(`{"id": "2r"}`)
	Expect(payloads[1]).To.Eql(`{"id": "9r"}`)
}

func (_ CacheTest) Fetch() {
	cache, _ := buildCache(1024, time.Second*10, 0)
	Expect(cache.Fetch(2, "page")).To.Eql(`{"id": "2rd"}`)
	Expect(cache.Fetch(2, "page")).To.Eql(`{"id": "2rd"}`)
}

func (_ CacheTest) FetchMatchestype() {
	cache, _ := buildCache(1024, time.Second*10, 0)
	Expect(cache.Fetch(2, "user")).To.Equal(nil)
}

func (_ CacheTest) FetchFallsBackToSummary() {
	cache, _ := buildCache(1024, time.Second*10, 0)
	Expect(cache.Fetch(9999999, "user")).To.Eql(`{"id": "9999999x"}`)
	Expect(cache.Fetch(9999999, "user")).To.Eql(`{"id": "9999999x"}`)
}

func (_ CacheTest) FetchFallbackMatchestype() {
	cache, _ := buildCache(1024, time.Second*10, 0)
	Expect(cache.Fetch(9999999, "page")).To.Equal(nil)
}

func (_ CacheTest) PreloadsTheCacheWithSummaries() {
	cache, _ := buildCache(1024, time.Second*10, 100)
	cache.fetcher = nilFetcher{}
	Expect(cache.Fetch(9999999, "user")).To.Eql(`{"id": "9999999x"}`)
	Expect(cache.Fetch(9999999, "user")).To.Eql(`{"id": "9999999x"}`)
}

func (_ CacheTest) PreloadsTheCacheWithDetails() {
	cache, _ := buildCache(1024, time.Second*10, 100)
	cache.fetcher = nilFetcher{}
	Expect(cache.Fetch(2, "page")).To.Eql(`{"id": "2rd"}`)
}

func buildCache(size uint64, ttl time.Duration, preload int) (*Cache, *NormalResult) {
	storage, err := newSqliteStorage("./test.db")
	if err != nil {
		panic(err)
	}
	cache, err := newCache(storage, Configure().CachePreload(preload).CacheSize(size).CacheTTL(ttl))
	if err != nil {
		panic(err)
	}
	result := newResult(cache, 10, 10)
	return cache, result
}

type nilFetcher struct {
}

func (f nilFetcher) LoadNResources(n int) (map[Id][][]byte, error) {
	return nil, nil
}

func (f nilFetcher) Fill([]interface{}, map[Id]int, [][]byte, []string, bool) error {
	return nil
}

func (f nilFetcher) Get(id Id, tpe string) ([]byte, bool) {
	return nil, false
}
