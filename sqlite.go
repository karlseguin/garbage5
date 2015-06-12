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
		statements[i] = "select payload, ? as s from resources where id = ?"
	}

	stmt, err := db.Prepare(strings.Join(statements, " union all "))
	if err != nil {
		return batcher{}, err
	}
	return batcher{stmt, count}, nil
}

type SqliteStorage struct {
	*sql.DB
	batchers []batcher
}

func newSqliteStorage(path string) (*SqliteStorage, error) {
	db, err := sql.Open("sqlite3", path)
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

	return &SqliteStorage{db, batchers}, nil
}

func (s *SqliteStorage) Fetch(ids []interface{}, payloads [][]byte) error {
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
			var payload []byte
			rows.Scan(&payload, &index)
			payloads[index] = payload
		}
		rows.Close()
		if l -= batcher.count; l == 0 {
			break
		}
		ids = ids[count:]
	}
	return nil
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

func (s *SqliteStorage) EachSet(onlyNew bool, f func(name string, ids []Id)) error {
	return s.each(onlyNew, 2, f)
}

func (s *SqliteStorage) EachList(onlyNew bool, f func(name string, ids []Id)) error {
	return s.each(onlyNew, 3, f)
}

func (s *SqliteStorage) ClearNew() error {
	_, err := s.DB.Exec("truncate table updated")
	return err
}

func (s *SqliteStorage) each(onlyNew bool, tpe int, f func(name string, ids []Id)) error {
	indexes, err := s.DB.Query("select id, payload from indexes where type = ?", tpe)
	if err != nil {
		return err
	}
	defer indexes.Close()

	for indexes.Next() {
		var id string
		var blob []byte
		indexes.Scan(&id, &blob)

		ids := make([]Id, len(blob)/4)
		for i := 0; i < len(blob); i += 4 {
			ids[i/4] = Id(encoder.Uint32(blob[i:]))
		}
		f(id, ids)
	}
	return nil
}

func (s *SqliteStorage) Close() error {
	return s.DB.Close()
}
