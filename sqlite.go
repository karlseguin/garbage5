package indexes

import (
	"database/sql"
	"encoding/binary"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var (
	encoder = binary.LittleEndian
)

type SqliteStorage struct {
	*sql.DB
}

func newSqliteStorage(path string) (*SqliteStorage, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &SqliteStorage{db}, nil
}

func (s *SqliteStorage) Fetch(miss []*Miss) error {
	l := len(miss)
	sids := make([]string, l)
	for i, m := range miss {
		sids[i] = strconv.Itoa(int(m.id))
	}
	rows, err := s.Query("select data from resources where id in (" + strings.Join(sids, ",") + ")")
	if err != nil {
		return err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		rows.Scan(&miss[i].payload)
		i++
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
