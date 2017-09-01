#lang racket

(require db
         rackunit
         rackunit/text-ui
         "db_api.rkt")

(define db-path "db_api_test.db")
(define test-user-schema "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
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

(define (raised-exn? fun)
  (with-handlers ([exn? (lambda (e) #t)])
    (begin
      (fun)
      #f)))

(define (test-exec-expect-exn testfun)
  (if (raised-exn? (lambda () (test-exec testfun)))
      null
      (error "Expected exception")))

(define (fetch-rows v)
  (cdr (fetch v)))

(define (check-rows v expected)
  (check-equal? expected (fetch-rows v)))

(define (insert-many v cols rows)
  (map (lambda (r) (insert v cols r)) rows))

(define (insert-test-names v)
  (insert-many v "name" '(("Ezra") ("Christos") ("Steve") ("Scott"))))

(define-test-suite db-api-tests
    (test-suite
        "Basic tests for db api"
        (test-case
          "Empty db returns no rows"
          (check-equal? 0 (length (test-exec fetch-rows))))
    )

    (test-suite
      "Tests for insert"
      (test-case
        "Inserting invalid column name fails"
        (test-exec-expect-exn (lambda (v) (insert v "FAIL" '("Ezra"))))
      )
      (test-case
        "Inserting with required row missing fails"
        (test-exec-expect-exn (lambda (v) (insert v "" '())))
        )
      (test-case
        "Insert provided vals must match number of given cols"
          (test-exec-expect-exn (lambda (v) (insert v "name" '(1 "Ezra"))))
        )
        (test-case
          "Insert into empty db succeeds"
          (test-exec (lambda (v)
              (begin
                (insert v "name" '("Ezra"))
                (check-rows v '((1 "Ezra"))
              ))
          )
        ))
        (test-case
          "Insert many then fetch from where-restricted view"
          (test-exec (lambda (v)
              (begin
                (insert-test-names v)
                (check-rows (where v "name < 'S' and name > 'D'") '((1 "Ezra")))
              )
          ))
        ))
)

(run-tests db-api-tests)
