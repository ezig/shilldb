#lang racket

(require db)
(require shilldb/private/out)
(require (for-syntax racket/syntax))

(define DBPATH "/Users/ezra/Dropbox/Shill/db_api/experiments/queries/test.db")

(define (repeater f count)
  (for ((i (in-range count)))
    (f)))

(define (random-range size)
  (let ([low (random 0 (1 . + . (100 . - . size)))])
    (values low (low . + . (size . - . 1)))))

(define-syntax (with-db stx)
  (syntax-case stx ()
    [(_ exp)
     (with-syntax ([db (format-id stx "~a" #'db)])
       #'(let ([db (sqlite3-connect #:database DBPATH)])
           (begin
             (let ([ret exp])
               (disconnect db)
               ret))))]))

(define-syntax (time stx)
  (syntax-case stx ()
    [(_ exp)
     #'(let ([start (current-milliseconds)])
         (begin
           (let ([res exp]
                 [end (current-milliseconds)])
             (- end start))))]))

(define (execute-where use-sdb? select)
  (let-values ([(low high) (random-range select)])
    (begin
      (if use-sdb?
          (fetch (where (open-view DBPATH "test") (format "value >= ~a and value <= ~a" low high)))
          (with-db (query-rows db "select * from test where value >= $1 and value <= $2" low high)))
      void)))

(define (execute-update-all-pass use-sdb? select)
  (let-values ([(low high) (random-range select)])
    (begin
      (if use-sdb?
          (update
           (where (open-view DBPATH "test") (format "value >= ~a and value <= ~a" low (+ high select)))
           (format "value = value + ~a" (/ select 2))
           (format "value >= ~a and value <= ~a" low high))
          (with-db (query-exec db "update test set value = value + $1 where value >= $2 and value <= $3" (/ select 2) low high)))
      void)))

(define (execute-update-fail use-sdb? select)
  (let-values ([(low high) (random-range select)])
    (begin
      (if use-sdb?
          (update
           (where (open-view DBPATH "test") (format "value >= ~a and value <= ~a" low high))
           (format "value = value + ~a" (/ select 2))
           (format "value >= ~a and value <= ~a" low high))
          (with-db (query-exec db "update test set value = value + $1 where value >= $2 and value <= $3" (/ select 2) low high)))
      void)))

(define (execute-insert use-sdb?)
  (let-values ([(low high) (random-range 20)]
               [(ntimes) 10])
  (begin
    (repeater
    (lambda () (if use-sdb?
        (insert
          (where (open-view DBPATH "test") (format "value >= ~a and value <= ~a" low (+ high 10)))
          "id, value"
          (list high (+ high 5)))
        (with-db (query-exec db "insert into test (id, value) values ($1, $2)" high (+ high 5)))))
    ntimes)
    void)))

(define (execute-delete use-sdb? select)
  (let-values ([(low high) (random-range select)])
    (begin
      (if use-sdb?
          (delete (where (open-view DBPATH "test") (format "value >= ~a and value <= ~a" low high)))
          (with-db (query-exec db "delete from test where value >= $1 and value <= $2" low high)))
      void)))

(define (execute-query type use-sdb? selectivity)
  (match type
    ['where (execute-where use-sdb? selectivity)]
    ['update (execute-update-all-pass use-sdb? selectivity)]
    ['insert (execute-insert use-sdb?)]
    ['delete (execute-delete use-sdb? selectivity)]
    ['updatefail (execute-update-fail use-sdb? selectivity)]))

(define (main)
  (command-line
   #:args (use-sdb selectivityarg querytypearg)
   (define use-sdb? (= (string->number use-sdb) 1))
   (define selectivity (string->number selectivityarg))
   (define querytype (string->symbol querytypearg))
   (println (time (execute-query querytype use-sdb? selectivity)))))

(random-seed 42)
(main)

