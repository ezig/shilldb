#lang racket

(provide execute-reserve
         execute-my-reservations
         execute-remove-reservation
         execute-num-reservations
         execute-search-author)

(require (planet dmac/spin))
(require db)
(require json)
(require (prefix-in sdb: shilldb/private/out))
(require (for-syntax racket/syntax))

(define DBPATH "/Users/ezra/Dropbox/Shill/db_api/experiments/library/test.db")
(define DBIMPL 'sdb)

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

(define (sdb-to-json data)
  (with-output-to-string
      (λ () (write-json (cdr data)))))

(define (sdb-row-to-json data)
  (with-output-to-string
      (λ () (write-json (first (cdr data))))))

(define/contract (reserve/ctc user book v)
  (->i ([user string?]
        [book string?]
        [v (user) (sdb:view/c [+insert #:restrict (λ (v) (sdb:where v (format "cardholder_id = ~a" user)))])])
       [res any/c])
  (sdb:insert v "cardholder_id, book" (list user book)))

(define (execute-reserve user book)
  (case DBIMPL
    ['db (with-db
            (query-exec db "insert into reservations (cardholder_id, book) values ($1, $2)" user book))]
    ['sdb (let ([v (sdb:open-view DBPATH "reservations")])
            (sdb:insert v "cardholder_id, book" (list user book)))]
    ['sdb-ctc (let ([v (sdb:open-view DBPATH "reservations")])
                (reserve/ctc user book v))]))

(define/contract (my-reservations/ctc user vr vb va)
  (->i ([user string?]
        [vr (user) (sdb:view/c [+fetch #:restrict (λ (v) (sdb:where v (format "cardholder_id = ~a" user)))] +join +where +select)]
        [vb (sdb:view/c +join +fetch +select +where)]
        [va (sdb:view/c +join +fetch +select +where)])
       [res string?])
  (sdb-to-json
   (sdb:fetch
    (sdb:select
     (sdb:where (sdb:join (sdb:join vr vb) va)
                (format "book = book_id and author = author_id and cardholder_id = ~a" user))
     "r_id, title, firstname, lastname"))))

(define (execute-my-reservations user)
  (case DBIMPL
    ['db (with-db
             (rows-to-json (query-rows db
                                       (string-append "select r_id, title, firstname, lastname "
                                                      "from reservations, books, authors "
                                                      "where book = book_id and author = author_id and cardholder_id = $1")
                                       user)))]
    ['sdb (let ([vr (sdb:open-view DBPATH "reservations")]
                [vb (sdb:open-view DBPATH "books")]
                [va (sdb:open-view DBPATH "authors")])
            (sdb-to-json
             (sdb:fetch
              (sdb:select
               (sdb:where (sdb:join (sdb:join vr vb) va)
                          (format "book = book_id and author = author_id and cardholder_id = ~a" user))
               "r_id, title, firstname, lastname"))))]
    ['sdb-ctc (let ([vr (sdb:open-view DBPATH "reservations")]
                    [vb (sdb:open-view DBPATH "books")]
                    [va (sdb:open-view DBPATH "authors")])
                (my-reservations/ctc user vr vb va))]))

(define/contract (remove-reservation/ctc user rid v)
  (->i ([user string?]
        [rid string?]
        [v (user) (sdb:view/c +where [+delete #:restrict (λ (v) (sdb:where v (format "cardholder_id = ~a" user)))])])
       [res any/c])
  (sdb:delete (sdb:where v (format "r_id = ~a" rid))))

(define (execute-remove-reservation user rid)
  (case DBIMPL
    ['db (with-db
               (query-exec db "delete from reservations where r_id = $1" rid))]
    ['sdb (let ([vr (sdb:open-view DBPATH "reservations")])
            (sdb:delete (sdb:where vr (format "r_id = ~a" rid))))]
    ['sdb-ctc (let ([vr (sdb:open-view DBPATH "reservations")])
                (remove-reservation/ctc user rid vr))]))

(define/contract (search-author/ctc fname lname vb va)
  (string? string? (sdb:view/c +fetch +join +select +where)  (sdb:view/c +fetch +join +select +where) . -> . string?)
  (sdb-to-json
             (sdb:fetch
              (sdb:select
               (sdb:where (sdb:join vb va)
                          (format "firstname = '~a' and lastname = '~a' and author = author_id" fname lname))
               "book_id, title"))))

(define (execute-search-author fname lname)
  (case DBIMPL
    ['db (with-db
             (rows-to-json (query-rows db
                                       (string-append "select book_id, title "
                                                      "from books, authors "
                                                      "where firstname = $1 and lastname = $2 and author = author_id")
                                                      fname lname)))]
    ['sdb (let ([vb (sdb:open-view DBPATH "books")]
                [va (sdb:open-view DBPATH "authors")])
            (sdb-to-json
             (sdb:fetch
              (sdb:select
               (sdb:where (sdb:join vb va)
                          (format "firstname = '~a' and lastname = '~a' and author = author_id" fname lname))
               "book_id, title"))))]
    ['sdb-ctc (let ([vb (sdb:open-view DBPATH "books")]
                    [va (sdb:open-view DBPATH "authors")])
                (search-author/ctc fname lname vb va))]))

(define/contract (num-reservations/ctc book v)
  (string? (sdb:view/c [+aggregate #:with (sdb:view/c +fetch)] +where) . -> . string?)
  (sdb-row-to-json (sdb:fetch (sdb:aggregate (sdb:where v (format "book = ~a" book)) "count(r_id)"))))

(define (execute-num-reservations book)
  (case DBIMPL
    ['db (with-db
             (row-to-json (query-row db "select count(*) from reservations where book = $1" book)))]
    ['sdb (let ([v (sdb:open-view DBPATH "reservations")])
            (sdb-row-to-json (sdb:fetch (sdb:aggregate (sdb:where v (format "book = ~a" book)) "count(r_id)"))))]
    ['sdb-ctc (let ([v (sdb:open-view DBPATH "reservations")])
                (num-reservations/ctc book v))]))

(get "/reserve"
     (lambda (req)
       (begin
         (let ([user (params req 'user)]
               [book (params req 'book)])
           (execute-reserve user book)))
         "OK"))

(get "/my_reservations"
     (lambda (req)
       (let ([user (params req 'user)])
         (execute-my-reservations user))))

(get "/remove_reservation"
     (lambda (req)
       (begin
         (let ([user (params req 'user)]
               [rid (params req 'rid)])
           (execute-remove-reservation user rid))
         "OK")))

(get "/search_author"
     (lambda (req)
       (let ([fname (params req 'fname)]
             [lname (params req 'lname)])
         (execute-search-author fname lname))))

(get "/num_reservations"
     (lambda (req)
       (let ([book (params req 'book)])
         (execute-num-reservations book))))

(run)