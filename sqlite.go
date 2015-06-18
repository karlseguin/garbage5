package indexes

import (
	"database/sql"
	"encoding/binary"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var (
	encoder = binary.LittleEndian
)

type batcher struct {
	*sql.Stmt
	count int
}

func newBatcher(db *sql.DB, count int) (batcher, error) {
	statements := make([]string, count)
	for i := 0; i < count; i++ {
		statements[i] = "select summary, ? as s from resources where id = ?"
	}

	stmt, err := db.Prepare(strings.Join(statements, " union all "))
	if err != nil {
		return batcher{}, err
	}
	return batcher{stmt, count}, nil
}

type SqliteStorage struct {
	*sql.DB
	get      *sql.Stmt
	batchers []batcher
}

func newSqliteStorage(path string) (*SqliteStorage, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	get, err := db.Prepare("select ifnull(details, summary) d from resources where id = ?")
	if err != nil {
		return nil, err
	}

	sizes := []int{25, 20, 15, 10, 5, 4, 3, 2, 1}
	batchers := make([]batcher, len(sizes))
	for i, size := range sizes {
		batcher, err := newBatcher(db, size)
		if err != nil {
			db.Close()
			return nil, err
		}
		batchers[i] = batcher
	}

	return &SqliteStorage{
		DB:       db,
		get:      get,
		batchers: batchers,
	}, nil
}

func (s *SqliteStorage) Get(id Id) (payload []byte) {
	s.get.QueryRow(id).Scan(&payload)
	return payload
}

func (s *SqliteStorage) Fill(ids []interface{}, payloads [][]byte) error {
	l := len(ids) / 2
	for true {
		var batcher batcher
		for _, batcher = range s.batchers {
			if batcher.count <= l {
				break
			}
		}
		count := batcher.count * 2
		rows, err := batcher.Query(ids[:count]...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var index int
			var summary []byte
			rows.Scan(&summary, &index)
			payloads[index] = summary
		}
		rows.Close()
		if l -= batcher.count; l == 0 {
			break
		}
		ids = ids[count:]
	}
	return nil
}

func (s *SqliteStorage) LoadNResources(n int) (map[Id][]byte, error) {
	m := make(map[Id][]byte, n)
	rows, err := s.DB.Query("select id, summary from resources order by random() limit ?", n)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int
		var summary []byte
		rows.Scan(&id, &summary)
		m[Id(id)] = summary
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
		return nil, err
	}

	var payload []byte
	err = s.DB.QueryRow("select payload from indexes where id = 'ids'").Scan(&payload)
	if err != nil {
		return nil, err
	}

	ids := make(map[string]Id, count)
	for len(payload) > 0 {
		l := int(payload[0])
		payload = payload[1:]
		id := string(payload[:l])
		payload = payload[l:]
		ids[id] = Id(encoder.Uint32(payload))
		payload = payload[IdSize:]
	}
	return ids, nil
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

		ids := make([]Id, len(blob)/IdSize)
		for i := 0; i < len(blob); i += IdSize {
			ids[i/IdSize] = Id(encoder.Uint32(blob[i:]))
		}
		f(id, ids)
	}
	return nil
}

func (s *SqliteStorage) Close() error {
	return s.DB.Close()
}
