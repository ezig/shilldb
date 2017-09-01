#lang racket

(require db
         "db_view.rkt")

(define/contract (make-sqlite3-view filename tablename)
  (-> string? string? dbview?)
  (let* ([cinfo (conn-info filename 'sqlite3)]
         ; Fix string-append hack for tableinfo to avoid injection attacks
         [table-query (format "PRAGMA table_info(~a)" tablename)]
         [tableinfo (connect-and-exec cinfo (λ (c) (query-rows c table-query)))]
         [columns (begin (for ([col tableinfo]
                               #:when (and (eq? 'num (type-to-sym 'sqlite3 (vector-ref col 2)))
                                           (not (sql-null? (vector-ref col 4)))))
                               (let ([new-default (string->number (vector-ref col 4))])
                                 (vector-set! col 4 new-default)))
                         (map (λ (col) (apply column (vector->list col))) tableinfo))]
         [column-hash (make-hash (map (λ (col) (cons (column-name col) col)) columns))]
         [type-map (make-hash (map (λ (col) (cons (column-name col)
                                       (type-to-sym 'sqlite3 (column-type col)))) columns))]
         [column-names (map (λ (col) (column-name col)) columns)]
         [t (table tablename type-map column-names column-hash)])
    (view cinfo t column-names empty-where column-names #t #t)))

(struct sqlite3-view (view conn-info)
  #:methods gen:dbview
  [(define (connect-and-exec dbview fun) 2)
   (define (get-create-trigger-query dbview) 2)
   (define (get-query dbview) 2)
   (define (get-delete-query dbview) 2)
   (define (get-update-query dbview) 2)
   (define (get-view dbview) 2)
   (define (copy-with-view dbview new-view) 2)])
