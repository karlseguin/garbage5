package indexes

import "time"

type Configuration struct {
	path         string
	maxSets      int
	maxResults   int
	cacheSize    int64
	cacheTTL     time.Duration
	cachePreload int
}

func Configure() *Configuration {
	return &Configuration{
		maxSets:      32,
		maxResults:   100,
		cachePreload: 5000,
		cacheSize:    64 * 1024 * 1024,
		cacheTTL:     time.Minute * 5,
		path:         "/tmp/indexes.db",
	}
}

// Path to persisted file
// [/tmp/indexes.db]
func (c *Configuration) Path(path string) *Configuration {
	c.path = path
	return c
}

// Maximum number of results we'll ever request from a query
// [100]
func (c *Configuration) MaxResults(max uint16) *Configuration {
	c.maxResults = int(max)
	return c
}

// The maximum number of sets a single query will be composed of
// [32]
func (c *Configuration) MaxSets(max uint8) *Configuration {
	c.maxSets = int(max)
	return c
}

// Size of the resource cache, in bytes
// [67108864] (64MB)
func (c *Configuration) CacheSize(size uint64) *Configuration {
	c.cacheSize = int64(size)
	return c
}

// TTL to store resources in the cache
// [5 minutes]
func (c *Configuration) CacheTTL(ttl time.Duration) *Configuration {
	c.cacheTTL = ttl
	return c
}

// Number of items to preload into the cache
// [5000]
func (c *Configuration) CachePreload(count int) *Configuration {
	c.cachePreload = count
	return c
}
