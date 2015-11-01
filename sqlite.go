package indexes

import (
	"database/sql"
	"encoding/binary"

	_ "gopkg.in/mattn/go-sqlite3.v1"
)

var (
	encoder = binary.LittleEndian
)

type SqliteStorage struct {
	*sql.DB
	iIndex *sql.Stmt
	uIndex *sql.Stmt
	dIndex *sql.Stmt
}

func newSqliteStorage(path string) (*SqliteStorage, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	iIndex, err := db.Prepare("insert into indexes (type, payload, id) values (?, ?, ?)")
	if err != nil {
		db.Close()
		return nil, err
	}
	uIndex, err := db.Prepare("update indexes set type = ?, payload = ? where id = ?")
	if err != nil {
		db.Close()
		return nil, err
	}
	dIndex, err := db.Prepare("delete from indexes where id = ?")
	if err != nil {
		db.Close()
		return nil, err
	}

	return &SqliteStorage{
		DB:     db,
		iIndex: iIndex,
		uIndex: uIndex,
		dIndex: dIndex,
	}, nil
}

func (s *SqliteStorage) ListCount() uint32 {
	count := 0
	s.DB.QueryRow("select count(*) from indexes where type = 3").Scan(&count)
	return uint32(count)
}

func (s *SqliteStorage) SetCount() uint32 {
	count := 0
	s.DB.QueryRow("select count(*) from indexes where type = 2").Scan(&count)
	return uint32(count)
}

func (s *SqliteStorage) LoadIds(newOnly bool) (map[string]Id, error) {
	var payload []byte
	err := s.DB.QueryRow("select payload from indexes where id = 'ids'").Scan(&payload)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return extractIdMap(payload), nil
}

func (s *SqliteStorage) EachSet(newOnly bool, f func(name string, ids []Id)) error {
	return s.each(newOnly, 2, f)
}

func (s *SqliteStorage) EachList(newOnly bool, f func(name string, ids []Id)) error {
	return s.each(newOnly, 3, f)
}

func (s *SqliteStorage) ClearNew() error {
	_, err := s.DB.Exec("truncate table updated")
	return err
}

func (s *SqliteStorage) each(newOnly bool, tpe int, f func(name string, ids []Id)) error {
	var indexes *sql.Rows
	var err error

	if newOnly {
		indexes, err = s.DB.Query("select id, payload from indexes where id in (select id from updated where type = ?)", tpe)
	} else {
		indexes, err = s.DB.Query("select id, payload from indexes where type = ?", tpe)
	}
	if err != nil {
		return err
	}
	defer indexes.Close()

	for indexes.Next() {
		var id string
		var blob []byte
		indexes.Scan(&id, &blob)

		f(id, extractIdsFromIndex(blob))
	}
	return nil
}

func (s *SqliteStorage) UpsertSet(id string, payload []byte) ([]Id, error) {
	return s.upsertIndex(id, 2, payload)
}

func (s *SqliteStorage) UpsertList(id string, payload []byte) ([]Id, error) {
	return s.upsertIndex(id, 3, payload)
}

func (s *SqliteStorage) RemoveList(id string) error {
	return s.RemoveSet(id)
}

func (s *SqliteStorage) RemoveSet(id string) error {
	_, err := s.dIndex.Exec(id)
	return err
}

func (s *SqliteStorage) UpdateIds(payload []byte) (map[string]Id, error) {
	if err := s.upsert(s.iIndex, s.uIndex, 1, payload, "ids"); err != nil {
		return nil, err
	}
	return extractIdMap(payload), nil
}

func (s *SqliteStorage) upsertIndex(id string, tpe int, payload []byte) ([]Id, error) {
	if err := s.upsert(s.iIndex, s.uIndex, tpe, payload, id); err != nil {
		return nil, err
	}
	return extractIdsFromIndex(payload), nil
}

func (s *SqliteStorage) upsert(insert *sql.Stmt, update *sql.Stmt, arguments ...interface{}) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}
	insert, update = tx.Stmt(insert), tx.Stmt(update)

	result, err := update.Exec(arguments...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	_, err = insert.Exec(arguments...)
	return err
}

func (s *SqliteStorage) Close() error {
	s.iIndex.Close()
	s.uIndex.Close()
	s.dIndex.Close()
	return s.DB.Close()
}

func extractIdsFromIndex(blob []byte) []Id {
	ids := make([]Id, len(blob)/IdSize)
	for i := 0; i < len(blob); i += IdSize {
		ids[i/IdSize] = Id(encoder.Uint32(blob[i:]))
	}
	return ids
}

func extractIdMap(payload []byte) map[string]Id {
	ids := make(map[string]Id)
	for len(payload) > 0 {
		l := int(payload[0])
		payload = payload[1:]
		id := string(payload[:l])
		payload = payload[l:]
		ids[id] = Id(encoder.Uint32(payload))
		payload = payload[IdSize:]
	}
	return ids
}
