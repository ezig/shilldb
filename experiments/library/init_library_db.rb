require 'rubygems'
require 'sqlite3'
require 'faker'

DBNAME = "test.db"
File.delete(DBNAME) if File.exists?DBNAME

DB = SQLite3::Database.new( DBNAME )

DB.execute("CREATE TABLE cardholders (
    card_id integer PRIMARY KEY AUTOINCREMENT,
    firstname string,
    lastname string
)")

DB.execute("CREATE TABLE authors (
    author_id integer PRIMARY KEY AUTOINCREMENT,
    firstname string,
    lastname string
)")

DB.execute("CREATE TABLE books (
    book_id integer PRIMARY KEY AUTOINCREMENT,
    author integer,
    title string,
    copies integer
)")

DB.execute("CREATE TABLE reservations (
    r_id integer PRIMARY KEY AUTOINCREMENT,
    book integer,
    cardholder_id integer
)")
 
r = Random.new

num_cardholders = 10000
cardholder_insert = "INSERT INTO cardholders (firstname, lastname) VALUES (?, ?)"

DB.transaction
for i in 0...num_cardholders do
    DB.execute(cardholder_insert, Faker::Name.first_name, Faker::Name.last_name)
end
DB.commit


num_authors = 20000
author_insert = "INSERT INTO authors (firstname, lastname) VALUES (?, ?)"

DB.transaction
for i in 0...num_authors do
    DB.execute(author_insert, Faker::Name.first_name, Faker::Name.last_name)
end
DB.commit


num_books = 200000
book_insert = "INSERT INTO books (title, author, copies) VALUES (?, ?, ?)"

DB.transaction
for i in 0...num_books do
    DB.execute(book_insert, Faker::Book.title, r.rand(1..num_authors), r.rand(1..10))
end
DB.commit


num_reservations = 20000
reservation_insert = "INSERT into reservations (book, cardholder_id) VALUES (?, ?)"

DB.transaction
for i in 0...num_reservations do
    DB.execute(reservation_insert, r.rand(1..num_books), r.rand(1..num_cardholders))
end
DB.commit

