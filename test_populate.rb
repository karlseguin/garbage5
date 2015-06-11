require 'sqlite3'

File.delete('test.db') if File.exists?('test.db')

db = SQLite3::Database.new 'test.db'

ids = ['1r', '2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r']

initialization = [
  'pragma journal_mode=off',
  'pragma synchronous=off',
  'create table resources (id, data)',
  'create table lists (id int, name text, sort)',
  'create table sets (id int, name text)',
  'create table names (id int, name text, type int)',
  'create table updated (id int, type int)',
  "insert into names (id, name, type) values (1, 'recent', 2)",
  "insert into names (id, name, type) values (2, 'large', 2)",
  "insert into updated (id, type) values (3, 1)",
  "insert into updated (id, type) values (4, 2)",
]
initialization.each{|s| db.execute(s)}

ids.each_index do |index|
  id = index + 1
  db.execute('insert into lists (id, name, sort) values (?, ?, ?)', [id, 1, index])
end

db.execute('insert into lists (id, name, sort) values (8, 4, 1)')
db.execute('insert into lists (id, name, sort) values (10, 4, 2)')

db.execute('insert into sets (id, name) values (1, 3)')
db.execute('insert into sets (id, name) values (4, 3)')
db.execute('insert into sets (id, name) values (10, 3)')

1005.times do |index|
  id = index+1
  db.execute('insert into lists (id, name, sort) values (?, ?, ?)', [id, 2, index])
end

sets = {
  '1' => ['2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '2' => ['3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '3' => ['4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '4' => ['5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '5' => ['6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '6' => ['1r'],
}

nameId = 5
sets.each do |name, values|
  db.execute("insert into names (id, name, type) values (?, ?, ?)", [nameId, name, 1])
  values.each do |id|
    id = ids.index(id)+1
    db.execute("insert into sets (id, name) values (?, ?)", [id, nameId])
  end
  nameId += 1
end
