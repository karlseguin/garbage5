package indexes

type Fetcher func(ids []uint32) ([][]byte, error)

type Resources struct {
	fetcher Fetcher
}

func newResources(fetcher Fetcher) *Resources {
	return &Resources{fetcher: fetcher}
}

func (r *Resources) Fetch(ids []uint32) ([][]byte, error) {
	return r.fetcher(ids)
}
