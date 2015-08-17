package indexes

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"strings"

	_ "gopkg.in/mattn/go-sqlite3.v1"
)

var (
	encoder = binary.LittleEndian
)

type batcher struct {
	*sql.Stmt
	count int
}

func newBatcher(sql string, db *sql.DB, count int) (batcher, error) {
	statements := make([]string, count)
	for i := 0; i < count; i++ {
		statements[i] = sql
	}

	stmt, err := db.Prepare(strings.Join(statements, " union all "))
	if err != nil {
		return batcher{}, err
	}
	return batcher{stmt, count}, nil
}

type SqliteStorage struct {
	*sql.DB
	get            *sql.Stmt
	iIndex         *sql.Stmt
	uIndex         *sql.Stmt
	dIndex         *sql.Stmt
	iResource      *sql.Stmt
	uResource      *sql.Stmt
	dResource      *sql.Stmt
	summaryBatcher Batcher
	detailsBatcher Batcher
}

func newSqliteStorage(path string) (*SqliteStorage, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	get, err := db.Prepare("select ifnull(details, summary) d, case when details is null then 0 else 1 end as detailed from resources where id = ?")
	if err != nil {
		db.Close()
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

	iResource, err := db.Prepare("insert into resources (summary, details, id) values (?, ?, ?)")
	if err != nil {
		db.Close()
		return nil, err
	}
	uResource, err := db.Prepare("update resources set summary = ?, details = ? where id = ?")
	if err != nil {
		db.Close()
		return nil, err
	}
	dResource, err := db.Prepare("delete from resources where id = ?")
	if err != nil {
		db.Close()
		return nil, err
	}

	sizes := []int{25, 20, 15, 10, 5, 4, 3, 2, 1}
	summaryBatcher, err := NewBatcher(db, "select id, summary from resources where id in #IN#", sizes...)
	if err != nil {
		db.Close()
		return nil, err
	}

	detailsBatcher, err := NewBatcher(db, "select id, details from resources where id in #IN#", sizes...)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &SqliteStorage{
		DB:             db,
		get:            get,
		iIndex:         iIndex,
		uIndex:         uIndex,
		dIndex:         dIndex,
		iResource:      iResource,
		uResource:      uResource,
		dResource:      dResource,
		summaryBatcher: summaryBatcher,
		detailsBatcher: detailsBatcher,
	}, nil
}

func (s *SqliteStorage) Get(id Id) (payload []byte, detailed bool) {
	s.get.QueryRow(id).Scan(&payload, &detailed)
	return payload, detailed
}

func (s *SqliteStorage) Fill(miss BatchMiss, count int, payloads [][]byte, detailed bool) error {
	batcher := s.summaryBatcher
	if detailed {
		batcher = s.detailsBatcher
	}

	query := batcher.For(miss.params[:count])

	for query.HasMore() {
		rows, err := query.Query()
		if err != nil {
			return err
		}
		for rows.Next() {
			var id Id
			var payload []byte
			rows.Scan(&id, &payload)

			index := -1
			for i, x := range miss.ids {
				if x == id {
					index = i
					break
				}
			}
			if index == -1 {
				rows.Close()
				return errors.New("failed to find query index")
			}

			payloads[miss.indexes[index]] = payload

			count--
			miss.ids[index] = miss.ids[count]
			miss.indexes[index] = miss.indexes[count]
			miss.ids = miss.ids[:count]
			miss.indexes = miss.indexes[:count]
		}
		rows.Close()
	}
	return nil
}

func (s *SqliteStorage) LoadNResources(n int) (map[Id][][]byte, error) {
	m := make(map[Id][][]byte, n)
	rows, err := s.DB.Query("select id, summary, details from resources order by id desc limit ?", n)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int
		var summary, details []byte
		rows.Scan(&id, &summary, &details)
		m[Id(id)] = [][]byte{summary, details}
	}
	return m, nil
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
	var count int
	err := s.DB.QueryRow("select count(*) from resources").Scan(&count)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	var payload []byte
	err = s.DB.QueryRow("select payload from indexes where id = 'ids'").Scan(&payload)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return extractIdMap(payload, count), nil
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

func (s *SqliteStorage) UpsertResource(id Id, summary []byte, details []byte) error {
	return s.upsert(s.iResource, s.uResource, summary, details, id)
}

func (s *SqliteStorage) RemoveList(id string) error {
	return s.RemoveSet(id)
}

func (s *SqliteStorage) RemoveSet(id string) error {
	_, err := s.dIndex.Exec(id)
	return err
}

func (s *SqliteStorage) RemoveResource(id Id) error {
	_, err := s.dResource.Exec(id)
	return err
}

func (s *SqliteStorage) UpdateIds(payload []byte, estimatedCount int) (map[string]Id, error) {
	if err := s.upsert(s.iIndex, s.uIndex, 1, payload, "ids"); err != nil {
		return nil, err
	}
	return extractIdMap(payload, estimatedCount), nil
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
	return s.DB.Close()
}

func extractIdsFromIndex(blob []byte) []Id {
	ids := make([]Id, len(blob)/IdSize)
	for i := 0; i < len(blob); i += IdSize {
		ids[i/IdSize] = Id(encoder.Uint32(blob[i:]))
	}
	return ids
}

func extractIdMap(payload []byte, count int) map[string]Id {
	ids := make(map[string]Id, count)
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
