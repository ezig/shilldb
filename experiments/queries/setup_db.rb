require 'rubygems'
require 'sqlite3'

DBNAME = "test.db"
File.delete(DBNAME) if File.exists?DBNAME

DB = SQLite3::Database.new( DBNAME )
DB.execute("CREATE TABLE test(id, value)")

r = Random.new

insert_query = "INSERT INTO test (id, value) VALUES (?, ?)"

DB.transaction

for i in 1..20000 do
    DB.execute(insert_query, i, r.rand(100))
end

DB.commit
