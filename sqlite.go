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
	s.DB.QueryRow("select count(*) from sqlite_master where type='table' and name like 'list_%'").Scan(&count)
	return uint32(count)
}

func (s *SqliteStorage) SetCount() uint32 {
	count := 0
	s.DB.QueryRow("select count(*) from sqlite_master where type='table' and name like 'set_%'").Scan(&count)
	return uint32(count)
}

func (s *SqliteStorage) EachSet(f func(name string, ids []Id)) error {
	return s.each("set_", "", f)
}

func (s *SqliteStorage) EachList(f func(name string, ids []Id)) error {
	return s.each("list_", " order by sort", f)
}

func (s *SqliteStorage) each(prefix, postfix string, f func(name string, ids []Id)) error {
	tables, err := s.DB.Query("select name from sqlite_master where type='table' and name like ?", prefix+"%")
	defer tables.Close()
	if err != nil {
		return err
	}
	for tables.Next() {
		var count int
		var tableName string
		tables.Scan(&tableName)
		tableName = `"` + tableName + `"`
		if err := s.DB.QueryRow("select count(*) from " + tableName).Scan(&count); err != nil {
			return err
		}

		ids := make([]Id, count)

		rows, err := s.DB.Query("select id from " + tableName + postfix)
		if err != nil {
			return err
		}
		for i := 0; rows.Next(); i++ {
			var id int
			rows.Scan(&id)
			ids[i] = Id(id)
		}
		rows.Close()
		itemName := tableName[len(prefix)+1:]
		itemName = itemName[:len(itemName)-1]
		f(itemName, ids)
	}
	return nil
}

func (s *SqliteStorage) Close() error {
	return s.DB.Close()
}
