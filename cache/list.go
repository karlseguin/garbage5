package cache

type List struct {
	head *Entry
	tail *Entry
}

func NewList() *List {
	tail := &Entry{id: 0}
	return &List{
		head: &Entry{id: 0, next: tail},
		tail: tail,
	}
}

func (l *List) PushToFront(entry *Entry) {
	l.Remove(entry)
	head := l.head
	next := head.next
	next.prev = entry
	entry.next = next
	entry.prev = head
	head.next = entry
}

func (l *List) Remove(entry *Entry) {
	if entry.prev == nil {
		return
	}
	entry.prev.next, entry.next.prev = entry.next, entry.prev
}
