package indexes

import "bytes"

type Updater struct {
	db      *Database
	scratch []byte
	buffer  *bytes.Buffer
	ids     map[string]Id
	sets    map[string]Changes
	lists   map[string]Changes
}

// For sets, the key of updated is the id, and the value is meaningless
// For lists, the key is index to insert in, and the value is the id
// For ids, the key is the string variant, the value is the Id to map to
// For ids, if the value is 0, that means delete the key variant
type Changes struct {
	deleted map[Id]struct{}
	updated map[Id]Id
}

func NewUpdater(db *Database) *Updater {
	return &Updater{
		db:    db,
		ids:   make(map[string]Id),
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

func (u *Updater) IdsUpdate(value string, id Id) {
	u.ids[value] = id
}

func (u *Updater) SetDelete(name string, id Id) {
	changes := u.get(name, u.sets)
	changes.deleted[id] = struct{}{}
}

func (u *Updater) ListDelete(name string, id Id) {
	changes := u.get(name, u.lists)
	changes.deleted[id] = struct{}{}
}

func (u *Updater) IdsDelete(value string) {
	u.ids[value] = 0
}

func (u *Updater) Commit() error {
	db := u.db
	u.scratch = make([]byte, 4)
	u.buffer = bytes.NewBuffer(make([]byte, 0, 5*1024*1024))

	sql := db.storage.(*SqliteStorage)
	tx, err := sql.Begin()
	if err != nil {
		return err
	}
	insert := tx.Stmt(sql.iIndex)

	for name, changes := range u.sets {
		u.buffer.Reset()
		db.setSet(name, u.serializeSet(name, changes))
		if _, err := insert.Exec(2, u.buffer.Bytes(), name); err != nil {
			tx.Rollback()
			return err
		}
	}

	for name, changes := range u.lists {
		u.buffer.Reset()
		db.setList(name, u.serializeList(name, changes))
		if _, err := insert.Exec(3, u.buffer.Bytes(), name); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	u.buffer.Reset()
	u.serializeIds(u.ids)
	if err := u.db.UpdateIds(u.buffer.Bytes()); err != nil {
		return err
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
func (u *Updater) serializeSet(name string, changes Changes) []Id {
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
	return extractIdsFromIndex(u.buffer.Bytes())
}

// Serializing a list isn't easy since we need to figure out the order and such.
func (u *Updater) serializeList(name string, changes Changes) []Id {
	index := Id(0)
	existing := u.db.GetList(name)
	added := make(map[Id]struct{}, len(changes.updated))
	existing.Each(false, func(id Id) bool {
		for {
			id, exists := changes.updated[index]
			if !exists {
				break
			}
			u.write(id)
			added[id] = struct{}{}
			index++
		}
		if _, exists := changes.deleted[id]; exists == false {
			if _, exists := added[id]; exists == false {
				u.write(id)
				added[id] = struct{}{}
				index++
			}
		}
		return true
	})

	// any updates we have remaining go at the end of our list
	for _, id := range changes.updated {
		if _, exists := added[id]; exists == false {
			u.write(id)
		}
	}
	return extractIdsFromIndex(u.buffer.Bytes())
}

func (u *Updater) serializeIds(ids map[string]Id) {
	existing := u.db.getIds()

	// only add existing ones that aren't in our change set
	for key, id := range existing {
		if _, exists := ids[key]; !exists {
			u.writeMap(key, id)
		}
	}

	// add the new values
	for key, id := range ids {
		if id != 0 { // skip deletes
			u.writeMap(key, id)
		}
	}
}

func (u *Updater) write(id Id) {
	encoder.PutUint32(u.scratch, uint32(id))
	u.buffer.Write(u.scratch)
}

func (u *Updater) writeMap(key string, id Id) {
	u.buffer.WriteByte(byte(len(key)))
	u.buffer.WriteString(key)
	u.write(id)
}
