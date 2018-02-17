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

(define (execute-update use-sdb? select)
  (execute-update #t 10)(let-values ([(low high) (random-range select)])
    (begin
      (if use-sdb?
          (update
           (where (open-view DBPATH "test") (format "value >= ~a and value <= ~a" low (+ high 10)))
           "value = value + 10"
           (format "value >= ~a and value <= ~a" low high))
          (with-db (query-exec db "update test set value = value + 10 where value >= $1 and value <= $2" low high)))
      void)))

(define (execute-insert use-sdb?)
  2)

(define (execute-delete use-sdb? select)
  2)

(define (execute-query type use-sdb? selectivity)
  (match type
    [where (execute-where use-sdb? selectivity)]
    [update (execute-update use-sdb? selectivity)]
    [insert (execute-insert use-sdb?)]
    [delete (execute-delete use-sdb? selectivity)]))

(define (main)
  (command-line
   #:args (use-sdb selectivityarg querytypearg)
   (define use-sdb? (= (string->number use-sdb) 1))
   (define selectivity (string->number selectivityarg))
   (define querytype (string->symbol querytypearg))
   (println (time (execute-query querytype use-sdb? selectivity)))))

(random-seed 42)
(main)