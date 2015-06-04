package indexes

import (
	"database/sql"
	"fmt"
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

func (s *SqliteStorage) Fetch(ids []uint32) ([][]byte, error) {
	l := len(ids)
	sids := make([]string, l)
	for i, id := range ids {
		sids[i] = strconv.Itoa(int(id))
	}
	rows, err := s.Query("select data from resources where id in (" + strings.Join(sids, ",") + ")")
	fmt.Println(strings.Join(sids, ","))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bytes := make([][]byte, l)
	i := 0
	for rows.Next() {
		rows.Scan(&bytes[i])
		i++
	}
	return bytes[:i], nil
}

func (s *SqliteStorage) IdCount() uint32 {
	count := 0
	s.DB.QueryRow("select count(*) from ids").Scan(&count)
	return uint32(count)
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

func (s *SqliteStorage) EachId(f func(external string, internet uint32)) error {
	rows, err := s.DB.Query("select eid, id from resources")
	if err != nil {
		return err
	}
	var external string
	var internal int
	for rows.Next() {
		rows.Scan(&external, &internal)
		f(external, uint32(internal))
	}
	return nil
}

func (s *SqliteStorage) EachSet(f func(name string, ids []uint32)) error {
	return s.each("set_", "", f)
}

func (s *SqliteStorage) EachList(f func(name string, ids []uint32)) error {
	return s.each("list_", " order by sort", f)
}

func (s *SqliteStorage) each(prefix, postfix string, f func(name string, ids []uint32)) error {
	tables, err := s.DB.Query("select name from sqlite_master where type='table' and name like ? limit 3", prefix+"%")
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

		ids := make([]uint32, count)

		rows, err := s.DB.Query("select id from " + tableName + postfix)
		if err != nil {
			return err
		}
		for i := 0; rows.Next(); i++ {
			var id int
			rows.Scan(&id)
			ids[i] = uint32(id)
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
