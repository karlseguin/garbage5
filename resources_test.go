package indexes

import (
	"strconv"
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
	result.add(20)
	result.add(321)
	result.fill()

	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(3)
	Expect(payloads[0]).To.Eql("1")
	Expect(payloads[1]).To.Eql("20")
	Expect(payloads[2]).To.Eql("321")
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
	result.add(495)
	result.fill()
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("234")
	Expect(payloads[1]).To.Eql("495")

	Expect(resources.bucket(495).get(495).value).To.Eql("495")
}

func (_ ResourcesTests) DoesntReturnExpiredItem() {
	resources, result := buildResources(1024, time.Second*-10)
	resources.set(2, []byte("234"))
	result.add(2)
	result.add(495)
	result.fill()
	payloads := result.Payloads()
	Expect(len(payloads)).To.Equal(2)
	Expect(payloads[0]).To.Eql("2")
	Expect(payloads[1]).To.Eql("495")
}

func (_ ResourcesTests) Fetch() {
	resources, _ := buildResources(1024, time.Second*10)
	Expect(resources.Fetch(94)).To.Eql("94")
	Expect(resources.Fetch(94)).To.Eql("94")
}

func buildResources(size uint64, ttl time.Duration) (*Resources, *NormalResult) {
	resources := newResources(FakeFetcher{}, Configure().CacheSize(size).CacheTTL(ttl))

	result := newResult(resources, 10, 10)
	return resources, result
}

type FakeFetcher struct {
}

func (f FakeFetcher) Fill(miss []interface{}, payloads [][]byte) error {
	for i := 0; i < len(miss); i += 2 {
		index := miss[i].(int)
		body := strconv.Itoa(int(miss[i+1].(Id)))
		payloads[index] = []byte(body)
	}
	return nil
}

func (f FakeFetcher) Get(id Id) []byte {
	return []byte(strconv.Itoa(int(id)))
}
