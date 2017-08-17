#lang racket

(require db
         rackunit
         rackunit/text-ui
         "db_api.rkt")

(define db-path "db_api_test.db")
(define test-user-schema "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
(define test-posts-schema "CREATE TABLE posts (userid INTEGER DEFAULT 0 NOT NULL, title TEXT, post TEXT)")

(define (create-db)
  (begin
      (display-to-file "" db-path))
      (create-schema))

(define (create-schema)
  (let ([conn (sqlite3-connect #:database db-path)])
    (begin
        (query-exec conn test-user-schema)
        (query-exec conn test-posts-schema))
        (disconnect conn)))
        

(define (cleanup-db)
  (delete-file db-path))

(define (test-exec testfun)
  (around
    (create-db)
    (define/contract v
        (dbview/c select/p update/p insert/p where/p fetch/p) (open-dbview db-path "users"))
    (testfun v)
    (cleanup-db)))

(define (fetch-rows v)
  (cdr (fetch v)))

(define db-api-tests
    (test-suite
        "Tests for db api"
        (test-case
          "Empty db returns no rows"
          (check-equal? 0 (length (test-exec fetch-rows))))))

(run-tests db-api-tests)
