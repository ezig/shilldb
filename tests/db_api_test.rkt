#lang racket

(require db
         rackunit
         rackunit/text-ui
         "test-utils.rkt"
         (only-in "../racket/private/api/db_api_impl.rkt"
                  [where-impl where]
                  [update-impl update]
                  [insert-impl insert]
                  [delete-impl delete]
                  [fetch-impl fetch]
                  [select-impl select]
                  [aggregate-impl aggregate]
                  [join-impl join]
                  [mask-impl mask]
                  [make-view-impl make-view]))

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
    (define v
        (make-view db-path "users"))
    (testfun v)
    (cleanup-db)))

(define (test-exec-expect-exn testfun error-msg)
  (check-fail (test-exec testfun) error-msg))

(define (fetch-rows v)
  (cdr (fetch v)))

(define (check-rows v expected)
  (check-equal? (fetch-rows v) expected))

(define (insert-many v cols rows)
  (map (lambda (r) (insert v cols r)) rows))

(define (insert-test-names v)
  (insert-many v "name" '(("Ezra") ("Christos") ("Steve") ("Scott"))))

(define-test-suite
  db-api-tests
  (test-suite
    "Basic tests for db api"
    (test-case
      "Empty db returns no rows"
      (check-equal? 0 (length (test-exec fetch-rows))))
    )
  
  (test-suite
   "Tests for select"
   (test-case
    "Selecting can project away other columns"
    (test-exec
     (λ (v)
       (begin
         (insert v "name" '("Ezra"))
         (check-rows (select v "id") '((1)))))))
   (test-case
    "Can select simple arithmetic expression over columns"
    (test-exec
     (λ (v)
       (begin
         (insert v "name" '("Ezra"))
         (check-rows (select v "2 * id") '((2)))))))
   (test-case
    "Selecting nonexistent column fails"
    (test-exec-expect-exn
     (λ (v)
         (select v "FAIL"))
     "select: undefined identifier FAIL")))

  (test-suite
   "Tests for mask"
   (test-case
    "Mask behaves like select in simple case"
    (test-exec
     (λ (v)
       (begin
         (insert v "name" '("Ezra"))
         (check-rows (mask v "id") '((1)))))))
   (test-case
    "Mask does not allow complex columns"
    (test-exec-expect-exn
     (λ (v)
       (mask v "id + 2"))
     "contract violation"))
   (test-case
    "Mask does not allow nonsense columns not in the underlying table"
    (test-exec-expect-exn
     (λ (v)
       (mask v "FAIL"))
     "contract violation"))
   (test-case
    "Mask fails when it would mask away every column in view"
    (test-exec-expect-exn
     (λ (v)
       (mask (select v "name") "id"))
     "no columns left in view"))
   (test-case
    "Mask works properly when not all columns in view"
    (test-exec
     (λ (v)
       (begin
         (insert v "name" '("Ezra"))
         (check-rows (mask (select v "id") "name, id") '((1))))))))

  (test-suite
   "Tests for aggregate"
   (test-case
    "Simple aggregation works"
    (test-exec
     (λ (v)
       (begin
         (insert-test-names v)
         (check-rows (aggregate v "SUM(id)") '((10)))))))
   (test-case
    "Simple group by works"
    (test-exec
     (λ (v)
       (begin
         (insert-many v "name" '(("Ezra") ("Ezra") ("Christos")))
         (check-rows (aggregate v "name, COUNT(name)" #:groupby "name")
                     '(("Christos" 1) ("Ezra" 2)))))))
   (test-case
    "Simple having group by works"
    (test-exec
     (λ (v)
       (begin
         (insert-many v "name" '(("Ezra") ("Ezra") ("Christos")))
         (check-rows (aggregate v "name, COUNT(name)"
                                #:groupby "name"
                                #:having "count(name) > 1")
                     '(("Ezra" 2))))))))
  
  (test-suite
    "Tests for insert"
    (test-case
      "Inserting invalid column name fails"
      (test-exec-expect-exn
       (lambda (v) (insert v "FAIL" '("Ezra")))
       "invalid column names")
      )
    (test-case
      "Inserting with required row missing fails"
      (test-exec-expect-exn
       (lambda (v) (insert v "" '()))
       "invalid defaults")
      )
    (test-case
      "Insert provided vals must match number of given cols"
      (test-exec-expect-exn (lambda (v) (insert v "name" '(1 "Ezra")))
                            "contract violation")
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
      )
    (test-case
      "Insert fails when row is not within view"
      (test-exec-expect-exn
        (lambda (v) (insert (where v "name = 'Larry'") "name" '("Ezra")))
        "insert: failed"
        )
      )
    )
  (test-suite
    "Tests for delete"
    (test-case
      "Deleting inserted rows succeeds"
      (test-exec (lambda (v)
                   (begin
                     (insert-test-names v)
                     (delete v)
                     (check-rows v '())
                     ))
                 )
      )
    (test-case
      "Deleting doesn't touch rows outside of the view"
      (test-exec (lambda (v)
                   (begin
                     (insert-test-names v)
                     (delete (where v "name != 'Ezra'"))
                     (check-rows v '((1 "Ezra")))
                     )
                   ))
      )
    (test-case
      "Deleting fails on non-deletable view"
      (test-exec-expect-exn (lambda (v)
                   (begin
                     ; Joined views are not deletable
                     (delete (join v v ""))
                     )
                   )
                            "contract violation")
      )
    )
  (test-suite
    "Tests for update"
    (test-case
      "Basic update succeeds"
      (test-exec (lambda (v)
                   (begin
                     (insert v "name" '("Ezra"))
                     (update v "name = 'Larry'")
                     (check-rows v '((1 "Larry")))
                     )
                   ))
      )
    (test-case
      "Update of non-existant column fails"
      (test-exec-expect-exn
        (lambda (v) (update v "flavor = 'Cherry'"))
        "undefined identifier"
        )
      )
    (test-case
      "Update does not affect columns outside of view"
      (test-exec (lambda (v)
                   (begin
                     (insert v "name" '("Ezra"))
                     (update (where v "name != 'Ezra'") "name = 'Larry'")
                     (check-rows v '((1 "Ezra")))
                     )
                   ))
      )
    (test-case
      "Update that would cause a row to leave the view fails"
      (test-exec-expect-exn
        (lambda (v)
          (begin
            (insert-test-names v)
            (update (where v "name = 'Ezra'") "name = 'Larry'")
            )
          )
        "update: failed"
        )
      )
    (test-case
      "Update only affect views within optional where cond"
      (test-exec
        (lambda (v)
          (begin
            (insert v "name" '("Ezra"))
            (update v "name = 'Larry'" "name != 'Ezra'")
            (check-rows v '((1 "Ezra")))
            )
          )
        )
      )
    (test-case
      "Update can cause row to exit optional where cond as long as it stays in view"
      (test-exec
        (lambda (v)
          (begin
            (insert v "name" '("Ezra"))
            (update (where v "name < 'Zebra'") "name = 'Larry'" "name = 'Ezra'")
            (check-rows v '((1 "Larry")))
            )
          )
        )
      )
    )
  )

(run-tests db-api-tests)
