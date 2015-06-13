package indexes

import (
	"testing"
	"time"

	. "github.com/karlseguin/expect"
)

type ResourcesTests struct {
}

func Test_Resources(t *testing.T) {
	Expectify(&ResourcesTests{}, t)
}

func (_ ResourcesTests) FetchesItems() {
	_, result := buildResources(1024, time.Second*10)
	result.add(1)
	result.add(2)
	result.add(4)
	result.fill()

	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(3)
	Expect(string(payloads[0])).To.Eql(`{"id": "1r"}`)
	Expect(string(payloads[1])).To.Eql(`{"id": "2r"}`)
	Expect(string(payloads[2])).To.Eql(`{"id": "4r"}`)
}

func (_ ResourcesTests) GetsItemsFromCache() {
	resources, result := buildResources(1024, time.Second*10)
	resources.fetcher = nil
	resources.set(2, []byte("33"))
	resources.set(4, []byte("44"))
	result.add(2)
	result.add(4)
	result.fill()
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("33")
	Expect(payloads[1]).To.Eql("44")
}

func (_ ResourcesTests) MixesCachedAndUncachedResults() {
	resources, result := buildResources(1024, time.Second*10)
	resources.set(2, []byte("234"))
	result.add(2)
	result.add(10)
	result.fill()
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("234")
	Expect(payloads[1]).To.Eql(`{"id": "10r"}`)

	Expect(resources.bucket(10).get(10).value).To.Eql(`{"id": "10r"}`)
}

func (_ ResourcesTests) DoesntReturnExpiredItem() {
	resources, result := buildResources(1024, time.Second*-10)
	resources.set(2, []byte("234"))
	result.add(2)
	result.add(9)
	result.fill()
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql(`{"id": "2r"}`)
	Expect(payloads[1]).To.Eql(`{"id": "9r"}`)
}

func (_ ResourcesTests) Fetch() {
	resources, _ := buildResources(1024, time.Second*10)
	Expect(resources.Fetch(2)).To.Eql(`{"id": "2r"}`)
	Expect(resources.Fetch(2)).To.Eql(`{"id": "2r"}`)
}

func buildResources(size uint64, ttl time.Duration) (*Resources, *NormalResult) {
	storage, err := newSqliteStorage("./test.db")
	if err != nil {
		panic(err)
	}
	resources := newResources(storage, Configure().CacheSize(size).CacheTTL(ttl))
	result := newResult(resources, 10, 10)
	return resources, result
}
