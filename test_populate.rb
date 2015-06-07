require 'redis'
require 'sqlite3'

File.delete('test.db') if File.exists?('test.db')
File.delete('dump.rdb') if File.exists?('dump.rdb')

db = SQLite3::Database.new 'test.db'
redis = Redis.new

ids = ['1r', '2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r']

initialization = [
  'create table resources (id, data)',
  'create table list_recent(id int, sort int);',
  'create table list_large(id int, sort int);',
]
initialization.each{|s| db.execute(s)}

ids.each_index do |index|
  id = index + 1
  db.execute('insert into list_recent (id, sort) values (?, ?)', [id, index])
  redis.zadd("list_recent", index, id)
  redis.sadd('lists', 'list_recent')
end


1005.times do |index|
  id = index+1
  db.execute('insert into list_large (id, sort) values (?, ?)', [id, index])
  redis.zadd('list_large', index, id)
  redis.sadd('lists', 'list_large')
end

sets = {
  '1' => ['2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '2' => ['3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '3' => ['4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '4' => ['5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '5' => ['6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '6' => ['1r'],
}

sets.each do |name, values|
  db.execute("create table set_#{name}(id int)")
  redis.sadd('sets', "set_#{name}")
  values.each do |id|
    id = ids.index(id)+1
    db.execute("insert into set_#{name} values (?)", id)
    redis.sadd("set_#{name}", id)
  end
end
