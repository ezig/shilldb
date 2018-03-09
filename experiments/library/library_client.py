import csv
import json
import random
import requests
import sys

server = "http://localhost:8000/"
authors_file = "authors.csv"
user = 1
authors = None

def my_reservations():
    requests.get(server + "my_reservations", {"user": user})

def lookup_book(book_id):
    requests.get(server + "num_reservations", {"book": book_id})

def reserve_random_book():
    book_id = random.randint(0, 200000)
    requests.get(server + "reserve", {"user": user, "book": book_id})

def lookup_author():
    author = random.choice(authors)
    fname = author[0]
    lname = author[1]

    author_books = json.loads(requests.get(server + "search_author", {"fname": fname, "lname": lname}).text)
    book_id = random.choice(author_books)[0]
    return book_id

def delete_one_reservation():
    my_reservations = requests.get(server + "my_reservations", {"user": user})
    json_data = json.loads(my_reservations.text)

    if len(json_data) > 0:
        rid = random.choice(json_data)[0]
        requests.get(server + "remove_reservation", {"user": user, "rid": rid})

def reserve_new_book():
    global authors 

    author = random.choice(authors)
    fname = author[0]
    lname = author[1]

    author_books = json.loads(requests.get(server + "search_author", {"fname": fname, "lname": lname}).text)
    book_id = random.choice(author_books)[0]

    num_reservations = json.loads(requests.get(server + "num_reservations", {"book": book_id}).text)
    if num_reservations[0] < 2:
        requests.get(server + "reserve", {"user": user, "book": book_id})

def run_mixed(n):
    for i in range(1, n):
        reserve_new_book()
        reserve_new_book()
        reserve_new_book()
        delete_one_reservation()

def run_read(n):
    for i in range(1, n):
        my_reservations()
        lookup_book(lookup_author())
        lookup_book(lookup_author())

def run_write(n):
    for i in range(1, n):
        reserve_random_book()
        reserve_random_book()
        reserve_random_book()
        delete_one_reservation()

def run_tests(n, workload):
    global authors

    with open(authors_file) as csvData:
        csvReader = csv.reader(csvData)
        authors = list(csvReader)

    random.seed(42)

    if workload == "mixed":
        run_mixed(n)
    elif workload == "read":
        run_read(n)
    elif workload == "write":
        run_write(n)
    else:
        print("Unrecognized workload")
        exit(1)

if __name__ == '__main__':
    run_tests(250, sys.argv[1])

