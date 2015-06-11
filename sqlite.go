package indexes

import (
	"database/sql"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
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
	s.DB.QueryRow("select count(*) from names where type = 2").Scan(&count)
	return uint32(count)
}

func (s *SqliteStorage) SetCount() uint32 {
	count := 0
	s.DB.QueryRow("select count(*) from names where type = 1").Scan(&count)
	return uint32(count)
}

func (s *SqliteStorage) EachSet(onlyNew bool, f func(name string, ids []Id)) error {
	return s.each(onlyNew, 1, "sets", "", f)
}

func (s *SqliteStorage) EachList(onlyNew bool, f func(name string, ids []Id)) error {
	return s.each(onlyNew, 2, "lists", " order by sort", f)
}

func (s *SqliteStorage) ClearNew() error {
	_, err := s.DB.Exec("truncate table updated")
	return err
}

func (s *SqliteStorage) each(onlyNew bool, tpe int, tableName string, order string, f func(name string, ids []Id)) error {
	var tables *sql.Rows
	var err error
	if onlyNew {
		tables, err = s.DB.Query("select id, name from updated where type = ?", tpe)
	} else {
		tables, err = s.DB.Query("select id, name from names where type = ?", tpe)
	}
	if err != nil {
		return err
	}
	defer tables.Close()
	for tables.Next() {
		var count int
		var nameId int
		var indexName string
		tables.Scan(&nameId, &indexName)
		if err := s.DB.QueryRow("select count(*) from "+tableName+" where name = ?", nameId).Scan(&count); err != nil {
			return err
		}

		ids := make([]Id, count)
		rows, err := s.DB.Query("select id from "+tableName+" where name = ? "+order, nameId)
		if err != nil {
			return err
		}
		for i := 0; rows.Next(); i++ {
			var id int
			rows.Scan(&id)
			ids[i] = Id(id)
		}
		rows.Close()
		f(indexName, ids)
	}
	return nil
}

func (s *SqliteStorage) Close() error {
	return s.DB.Close()
}
