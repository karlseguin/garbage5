package garbage5

type Configuration struct {
	path       string
	maxResults int
	cacheSize  int
}

func Configure() *Configuration {
	return &Configuration{
		maxResults: 100,
		cacheSize:  67108864,
		path:       "/tmp/garbage5.db",
	}
}

// Path to persisted file
// [/tmp/garbage5.db]
func (c *Configuration) Path(path string) *Configuration {
	c.path = path
	return c
}

// Maximum number of results we'll ever request from a query
// [100]
func (c *Configuration) MaxResults(max uint32) *Configuration {
	c.maxResults = int(max)
	return c
}

// The size, in bytes, to reserve for a caching resources
// [67108864 (64MB)
func (c *Configuration) CacheSize(size uint64) *Configuration {
	c.cacheSize = int(size)
	return c
}
