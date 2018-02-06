#lang racket

(require json)
(require simple-csv/csv-reader)
(require "library_server.rkt")

(define file-path "/Users/ezra/Dropbox/Shill/db_api/experiments/library/authors.csv")
(define f (open-input-file file-path #:mode 'text))
(define authors (map ((curry map) string-trim) (row-gen->list ((make-csv-reader) f))))

(define USER "1")

(define (get-json s)
  (with-input-from-string
      s
    (λ () (read-json))))

(define (repeater f count)
  (for ((i (in-range count)))
    (f)))

(define (delete-one-reservation)
  (define jdata (get-json (execute-my-reservations USER)))
  (if ((length jdata) . > . 0)
      (execute-remove-reservation USER (~a (caar (shuffle jdata))))
      null))

(define (reserve-new-book)
  (define author (car (shuffle authors)))
  (define author-books (get-json (execute-search-author (first author) (second author))))
  (define book-id (car (car (shuffle author-books))))
  (define num-reservations (car (car (get-json (execute-num-reservations (~a book-id))))))
  (if (num-reservations . < . 2)
      (execute-reserve USER (~a book-id))
      null))

(random-seed 42)

(reserve-new-book)
  
(repeater
 (λ ()
   (begin
     (reserve-new-book)
     (reserve-new-book)
     (reserve-new-book)
     (delete-one-reservation)))
 50)