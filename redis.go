package indexes

import (
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
)

type RedisStorage struct {
	*redis.Pool
}

func newRedisStorage(address string) (*RedisStorage, error) {
	pool := redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", address)
	}, 10)
	return &RedisStorage{pool}, nil
}

func (r *RedisStorage) ListCount() uint32 {
	conn := r.Get()
	defer conn.Close()
	i, _ := redis.Int(conn.Do("scard", "lists"))
	return uint32(i)
}

func (r *RedisStorage) SetCount() uint32 {
	conn := r.Get()
	defer conn.Close()
	i, _ := redis.Int(conn.Do("scard", "sets"))
	return uint32(i)
}

func (r *RedisStorage) ClearNew() error {
	conn := r.Get()
	defer conn.Close()
	_, err := redis.Int(conn.Do("del", "updated_lists", "updated_sets"))
	return err
}

func (r *RedisStorage) EachSet(onlyNew bool, f func(name string, ids []Id)) error {
	conn := r.Get()
	defer conn.Close()

	index := "sets"
	if onlyNew {
		index = "updated_sets"
	}
	names, err := redis.Strings(conn.Do("smembers", index))
	if err != nil {
		return err
	}
	for _, name := range names {
		count, err := redis.Int(conn.Do("scard", name))
		if err != nil {
			return err
		}
		ids := make([]Id, count)
		values, err := redis.Ints(conn.Do("smembers", name))
		if err != nil {
			return err
		}
		for i, value := range values {
			ids[i] = Id(value)
		}
		f(name[4:], ids)
	}
	return nil
}

func (r *RedisStorage) EachList(onlyNew bool, f func(name string, ids []Id)) error {
	conn := r.Get()
	defer conn.Close()

	index := "lists"
	if onlyNew {
		index = "updated_lists"
	}
	names, err := redis.Strings(conn.Do("smembers", index))
	if err != nil {
		return err
	}

	for _, name := range names {
		var count int
		var isSet bool

		if name[:2] == "sl" {
			count, err = redis.Int(conn.Do("scard", name))
			isSet = true
		} else {
			count, err = redis.Int(conn.Do("zcard", name))
			isSet = false
		}
		if err != nil {
			return err
		}

		var values []int
		var prefix string
		ids := make([]Id, count)

		if isSet {
			prefix = "slist"
			field := name[strings.Index(name, ":by:")+4:]
			values, err = redis.Ints(conn.Do("sort", name, "by", "r:*->"+field, "alpha"))
		} else {
			prefix = "list"
			values, err = redis.Ints(conn.Do("zrange", name, 0, -1))
		}
		if err != nil {
			return err
		}
		for i, value := range values {
			ids[i] = Id(value)
		}
		f(name[len(prefix)+1:], ids)
	}
	return nil
}

func (r *RedisStorage) Fetch(miss []*Miss) error {
	conn := r.Get()
	defer conn.Close()

	conn.Send("multi")
	for _, m := range miss {
		conn.Send("hget", "r:"+strconv.Itoa(int(m.id)), "payload")
	}
	values, err := conn.Do("exec")
	if err != nil {
		return err
	}
	for i, value := range values.([]interface{}) {
		if value == nil {
			miss[i].payload = DefaultPayload
		} else {
			miss[i].payload = value.([]byte)
		}
	}
	return nil
}

func (r *RedisStorage) Close() error {
	return r.Pool.Close()
}
