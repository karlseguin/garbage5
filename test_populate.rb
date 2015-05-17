require 'sqlite3'

File.delete('test.db') if File.exists?('test.db')
db = SQLite3::Database.new 'test.db'
ids = ['1r', '2r', '3r', '4r', '5r', '6r', '7r', '8r', '9r', '10r', '11r', '12r', '13r', '14r', '15r']

initialization = [
  'create table ids(external varchar(50), internal int);',
  'create table list_recent(id int, sort int);',
  'create table list_large(id int, sort int);',
]
initialization.each{|s| db.execute(s)}

ids.each_index do |index|
  db.execute('insert into ids (external, internal) values (?, ?)', [ids[index], index+1])
  db.execute('insert into list_recent (id, sort) values (?, ?)', [index+1, index])
end

1005.times do |index|
  db.execute('insert into list_large (id, sort) values (?, ?)', [index+1, index])
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
  values.each do |id|
    db.execute("insert into set_#{name} values (?)", ids.index(id)+1)
  end
end