package garbage5

type Configuration struct {
	path       string
	maxResults int
}

func Configure() *Configuration {
	return &Configuration{
		maxResults: 100,
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
