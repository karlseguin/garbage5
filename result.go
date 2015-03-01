package garbage5

type Result interface {
	Release()
	Len() int
	Ids() []uint32
	Resources() [][]byte
	HasMore() bool
}

type ResourceFetcher func(id uint32) []byte

type ResultPool struct {
	list chan *NormalResult
}

func NewResultPool(max, count int, fetcher ResourceFetcher) *ResultPool {
	pool := &ResultPool{
		list: make(chan *NormalResult, count),
	}
	for i := 0; i < count; i++ {
		pool.list <- &NormalResult{
			pool:      pool,
			fetcher:   fetcher,
			ids:       make([]uint32, max),
			resources: make([][]byte, max),
		}
	}
	return pool
}

func (p *ResultPool) Checkout() *NormalResult {
	return <-p.list
}

type NormalResult struct {
	length    int
	more      bool
	ids       []uint32
	fetcher   ResourceFetcher
	resources [][]byte
	pool      *ResultPool
}

func (r *NormalResult) Add(id uint32) bool {
	resource := r.fetcher(id)
	if resource == nil {
		return false
	}
	r.ids[r.length] = id
	r.resources[r.length] = resource
	r.length += 1
	return true
}

func (r *NormalResult) Len() int {
	return r.length
}

func (r *NormalResult) Ids() []uint32 {
	return r.ids[:r.length]
}

func (r *NormalResult) Resources() [][]byte {
	return r.resources[:r.length]
}

func (r *NormalResult) HasMore() bool {
	return r.more
}

func (r *NormalResult) Release() {
	r.length = 0
	r.more = false
	r.pool.list <- r
}
