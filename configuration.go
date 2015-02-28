package garbage5

type Configuration struct {
	path string
}

func Configure() *Configuration {
	return &Configuration{
		path: "/tmp/garbage5.db",
	}
}

// Path to persisted file
// [/tmp/garbage5.db]
func (c *Configuration) Path(path string) *Configuration {
	c.path = path
	return c
}
