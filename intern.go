package indexes

import "sync"

// never releases any memory
// keep an eye on : https://github.com/golang/go/issues/5160

var (
	internLock   = new(sync.RWMutex)
	internLookup = make(map[string]int)
)

func intern(value string) int {
	internLock.RLock()
	n, exists := internLookup[value]
	internLock.RUnlock()
	if exists {
		return n
	}

	internLock.Lock()
	n, exists = internLookup[value]
	if exists == false {
		n = len(internLookup)
		internLookup[value] = n
	}
	internLock.Unlock()
	return n
}
