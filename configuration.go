package indexes

type Configuration struct {
	path       string
	maxSets    int
	maxResults int
}

func Configure() *Configuration {
	return &Configuration{
		maxSets:    32,
		maxResults: 100,
		path:       "/tmp/indexes.db",
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
