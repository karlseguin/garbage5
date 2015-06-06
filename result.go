package indexes

type Result interface {
	Release()
	Len() int
	Ids() []uint32
	HasMore() bool
	Payloads() [][]byte
}

var (
	EmptyResult = new(emptyResult)
)

type Ranked struct {
	id   uint32
	rank uint32
}

type Ranks []Ranked

func (r Ranks) Len() int {
	return len(r)
}

func (r Ranks) Less(i, j int) bool {
	return r[i].rank < r[j].rank
}

func (r Ranks) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type NormalResult struct {
	length    int
	more      bool
	ranked    Ranks
	query     *Query
	ids       []uint32
	misses    []*Miss
	payloads  [][]byte
	resources *Resources
}

func newResult(resources *Resources, maxSets int, maxResults int) *NormalResult {
	result := &NormalResult{
		resources: resources,
		ids:       make([]uint32, maxResults),
		misses:    make([]*Miss, maxResults),
		payloads:  make([][]byte, maxResults),
		ranked:    make(Ranks, SmallSetTreshold),
	}
	for i := 0; i < maxResults; i++ {
		result.misses[i] = new(Miss)
	}
	return result
}

func (r *NormalResult) add(id uint32) {
	r.ids[r.length] = id
	r.length += 1
}

func (r *NormalResult) addranked(id uint32, rank uint32) {
	r.ranked[r.length] = Ranked{id, rank}
	r.length += 1
}

func (r *NormalResult) Len() int {
	return r.length
}

func (r *NormalResult) Ids() []uint32 {
	return r.ids[:r.length]
}

func (r *NormalResult) Payloads() [][]byte {
	return r.payloads[:r.length]
}

func (r *NormalResult) HasMore() bool {
	return r.more
}

func (r *NormalResult) Release() {
	r.length = 0
	r.more = false
	r.query.release()
}

func (r *NormalResult) fill() (Result, error) {
	if err := r.resources.Fill(r); err != nil {
		r.Release()
		return EmptyResult, err
	}
	return r, nil
}

type emptyResult struct {
}

func (r *emptyResult) Len() int {
	return 0
}

func (r *emptyResult) Ids() []uint32 {
	return nil
}

func (r *emptyResult) Payloads() [][]byte {
	return nil
}

func (r *emptyResult) HasMore() bool {
	return false
}

func (r *emptyResult) Release() {
}
