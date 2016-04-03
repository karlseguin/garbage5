package indexes

import "bytes"

type Updater struct {
	db      *Database
	scratch []byte
	buffer  *bytes.Buffer
	sets    map[string]Changes
	lists   map[string]Changes
}

// For sets, the key of updated is the id, and the value is meaningless
// For lists, the key is index to insert in, and the value is the id
type Changes struct {
	deleted map[Id]struct{}
	updated map[Id]Id
}

func NewUpdater(db *Database) *Updater {
	return &Updater{
		db:    db,
		sets:  make(map[string]Changes),
		lists: make(map[string]Changes),
	}
}

func (u *Updater) SetUpdate(name string, id Id) {
	changes := u.get(name, u.sets)
	changes.updated[id] = id
}

func (u *Updater) ListUpdate(name string, id Id, index Id) {
	changes := u.get(name, u.lists)
	changes.updated[index] = id
}

func (u *Updater) SetDelete(name string, id Id) {
	changes := u.get(name, u.sets)
	changes.deleted[id] = struct{}{}
}

func (u *Updater) ListDelete(name string, id Id) {
	changes := u.get(name, u.lists)
	changes.deleted[id] = struct{}{}
}

func (u *Updater) Commit() error {
	u.scratch = make([]byte, 4)
	u.buffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))

	for name, changes := range u.sets {
		u.buffer.Reset()
		u.serializeSet(name, changes)
		if err := u.db.UpdateSet(name, u.buffer.Bytes()); err != nil {
			return err
		}
	}

	for name, changes := range u.lists {
		u.buffer.Reset()
		u.serializeList(name, changes)
		if err := u.db.UpdateList(name, u.buffer.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func (u *Updater) get(name string, container map[string]Changes) Changes {
	if changes, exists := container[name]; exists {
		return changes
	}
	changes := Changes{
		updated: make(map[Id]Id),
		deleted: make(map[Id]struct{}),
	}
	container[name] = changes
	return changes
}

// Serializing a set is pretty simple. We take the existing set, serialize
// each id which we don't want to delete and add to that any new ids that don't
// already exists.
func (u *Updater) serializeSet(name string, changes Changes) {
	existing := u.db.GetSet(name)

	// serialize the existing values, except those we want to delete
	existing.Each(true, func(id Id) bool {
		if _, exists := changes.deleted[id]; exists == false {
			u.write(id)
		}
		return true
	})

	// serialize the new values, except those that are already existing
	for id := range changes.updated {
		if existing.Exists(id) == false {
			u.write(id)
		}
	}
}

// Serializing a list isn't easy since we need to figure out the order and such.
func (u *Updater) serializeList(name string, changes Changes) {
	existing := u.db.GetList(name)

	index := Id(0)
	existing.Each(false, func(id Id) bool {
		for {
			id, exists := changes.updated[index]
			if !exists {
				break
			}
			u.write(id)
			delete(changes.updated, index)
			index++
		}
		if _, exists := changes.deleted[id]; exists == false {
			u.write(id)
			index++
		}
		return true
	})

	// any updates we have remaining go at the end of our list
	for _, id := range changes.updated {
		u.write(id)
	}
}

func (u *Updater) write(id Id) {
	encoder.PutUint32(u.scratch, uint32(id))
	u.buffer.Write(u.scratch)
}
