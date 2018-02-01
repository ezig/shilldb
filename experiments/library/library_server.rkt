#lang racket

(require (planet dmac/spin))
(require db)
(require json)
(require (for-syntax racket/syntax))

(define DBPATH "/Users/ezra/Dropbox/Shill/db_api/experiments/library/test.db")

(define-syntax (with-db stx)
  (syntax-case stx ()
    [(_ exp)
        (with-syntax ([db (format-id stx "~a" #'db)])
          #'(let ([db (sqlite3-connect #:database DBPATH)])
              (begin
                (let ([ret exp])
                  (disconnect db)
                  ret))))]))

(define (rows-to-json rows)
  (with-output-to-string
      (λ () (write-json (map vector->list rows)))))

(define (row-to-json row)
  (with-output-to-string
      (λ () (write-json (vector->list row)))))


(get "/reserve"
     (lambda (req)
       (begin
         (let ([user (params req 'user)]
               [book (params req 'book)])
           (with-db
               (query-exec db "insert into reservations (cardholder_id, book) values ($1, $2)" user book)))
         "OK")))

(get "/my_reservations"
     (lambda (req)
       (let ([user (params req 'user)])
         (with-db
             (rows-to-json (query-rows db
                                       (string-append "select r_id, title, firstname, lastname "
                                                      "from reservations, books, authors "
                                                      "where book = book_id and author = author_id and cardholder_id = $1")
                                       user))))))

(get "/remove_reservation"
     (lambda (req)
       (begin
         (let ([user (params req 'user)]
               [rid (params req 'rid)])
           (with-db
               (query-exec db "delete from reservations where r_id = $1" rid)))
         "OK")))

(get "/search_author"
     (lambda (req)
       (let ([fname (params req 'fname)]
             [lname (params req 'lname)])
         (with-db
             (rows-to-json (query-rows db
                                       (string-append "select book_id, title "
                                                      "from books, authors "
                                                      "where firstname = $1 and lastname = $2 and author = author_id"
                                                      fname lname)))))))

(get "/search_book"
     (lambda (req) "Hello!"))

(get "/num_reservations"
     (lambda (req)
       (let ([book (params req 'book)])
         (with-db
             (row-to-json (query-row db "select count(*) from reservations where book = $1" book))))))

(run)