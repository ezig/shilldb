import csv
import json
import random
import requests

server = "http://localhost:8000/"
authors_file = "authors.csv"
user = 1
authors = None

def delete_one_reservation():
    my_reservations = requests.get(server + "my_reservations", {"user": user})
    json_data = json.loads(my_reservations.text)

    if len(json_data) > 0:
        rid = random.choice(json_data)[0]
        requests.get(server + "remove_reservation", {"user": user, "rid": rid})

def reserve_new_book():
    author = random.choice(authors)
    fname = author[0]
    lname = author[1]

    author_books = json.loads(requests.get(server + "search_author", {"fname": fname, "lname": lname}).text)
    book_id = random.choice(author_books)[0]

    num_reservations = json.loads(requests.get(server + "num_reservations", {"book": book_id}).text)
    if num_reservations[0] < 2:
        requests.get(server + "reserve", {"user": user, "book": book_id})

if __name__ == '__main__':
    with open(authors_file) as csvData:
        csvReader = csv.reader(csvData)
        authors = list(csvReader)

    random.seed(42)

    for i in range(1, 500):
        reserve_new_book()
        reserve_new_book()
        reserve_new_book()
        delete_one_reservation()
