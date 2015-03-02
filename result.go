package garbage5

type Result interface {
	Release()
	Len() int
	Ids() []uint32
	Resources() [][]byte
	HasMore() bool
}

type ResultPool struct {
	list chan *NormalResult
}

func NewResultPool(max, count int) *ResultPool {
	pool := &ResultPool{
		list: make(chan *NormalResult, count),
	}
	for i := 0; i < count; i++ {
		pool.list <- &NormalResult{
			pool:      pool,
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
	resources [][]byte
	pool      *ResultPool
}

func (r *NormalResult) Add(id uint32, resource []byte) {
	r.ids[r.length] = id
	r.resources[r.length] = resource
	r.length += 1
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
