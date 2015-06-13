require 'sqlite3'

File.delete('test.db') if File.exists?('test.db')

db = SQLite3::Database.new 'test.db'

ids = ['1r', '2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r']

initialization = [
  'pragma journal_mode=off',
  'pragma synchronous=off',
  'create table resources (id, payload)',
  'create table indexes (id string, payload blob, type int)',
  # 'create table updated (id int, type int)',
  # "insert into updated (id, type) values (3, 1)",
  # "insert into updated (id, type) values (4, 2)",
]
initialization.each{|s| db.execute(s)}

map = ''
indexes = []
ids.each_index do |index|
  indexes << index+1
  eid = ids[index-1]
  map += [eid.length].pack('c')
  map += eid
  map += [index].pack('V')
end
db.execute('insert into indexes (id, payload, type) values (?, ?, ?)', ['recent', indexes.pack('V*'), 3])
db.execute('insert into indexes (id, payload, type) values (?, ?, ?)', ['ids', map, 1])
# db.execute('insert into lists (id, name, sort) values (8, 4, 1)')
# db.execute('insert into lists (id, name, sort) values (10, 4, 2)')
#
# db.execute('insert into sets (id, name) values (1, 3)')
# db.execute('insert into sets (id, name) values (4, 3)')
# db.execute('insert into sets (id, name) values (10, 3)')

indexes = []
1005.times do |index|
  indexes << index+1
end
db.execute('insert into indexes (id, payload, type) values (?, ?, ?)', ['large', indexes.pack('V*'), 3])

sets = {
  '1' => ['2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '2' => ['3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '3' => ['4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '4' => ['5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '5' => ['6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r'],
  '6' => ['1r'],
}

nameId = 1
sets.each do |name, values|
  indexes = []
  values.each do |id|
    indexes << ids.index(id)+1
  end
  db.execute('insert into indexes (id, payload, type) values (?, ?, ?)', [nameId, indexes.pack('V*'), 2])
  nameId += 1
end
