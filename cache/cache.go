package cache

const (
	BUCKETS     = 16
	BUCKET_MASK = BUCKETS - 1
)

type Entry struct {
	id   uint32
	data []byte
	prev *Entry
	next *Entry
}

type Cache struct {
	list        *List
	maxSize     int
	size        int
	buckets     []*bucket
	deletables  chan *Entry
	promotables chan *Entry
}

func New(maxSize int) *Cache {
	c := &Cache{
		maxSize:     maxSize,
		list:        NewList(),
		buckets:     make([]*bucket, BUCKETS),
		deletables:  make(chan *Entry, 1024),
		promotables: make(chan *Entry, 1024),
	}
	for i := 0; i < BUCKETS; i++ {
		c.buckets[i] = &bucket{lookup: make(map[uint32]*Entry)}
	}
	go c.worker()
	return c
}

func (c *Cache) Get(id uint32) []byte {
	bucket := c.bucket(id)
	entry := bucket.get(id)
	if entry == nil {
		return nil
	}
	c.promotables <- entry
	return entry.data
}

func (c *Cache) Set(id uint32, data []byte) {
	entry := &Entry{
		id:   id,
		data: data,
	}
	existing := c.bucket(id).set(id, entry)
	if existing != nil {
		c.deletables <- existing
	}
	c.promotables <- entry
}

func (c *Cache) Update(id uint32, data []byte) {
	c.bucket(id).update(id, data)
}

func (c *Cache) Delete(id uint32) {
	if existing := c.bucket(id).delete(id); existing != nil {
		c.deletables <- existing
	}
}

func (c *Cache) bucket(id uint32) *bucket {
	return c.buckets[id&BUCKET_MASK]
}

func (c *Cache) worker() {
	for {
		select {
		case entry := <-c.promotables:
			if entry.prev == nil { //new item
				c.size += len(entry.data)
				if c.size > c.maxSize {
					c.gc()
				}
			}
			c.list.PushToFront(entry)
		case entry := <-c.deletables:
			if entry.prev != nil {
				c.list.Remove(entry)
				c.size -= len(entry.data)
			}
		}
	}
}

func (c *Cache) gc() {
	for i := 0; i < 1000; i++ {
		entry := c.list.tail.prev
		if entry == nil || entry.data == nil {
			return
		}
		c.bucket(entry.id).delete(entry.id)
		c.list.Remove(entry)
		c.size -= len(entry.data)
	}
}
