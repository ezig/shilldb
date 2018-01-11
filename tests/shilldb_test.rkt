#lang racket

(require
  db
  rackunit
  rackunit/text-ui
  "../racket/private/shilldb-macros.rkt"
  "test-utils.rkt"
  (except-in "../racket/private/shilldb.rkt"
             view/c))

; https://stackoverflow.com/questions/16842811/racket-how-to-retrieve-the-path-of-the-running-file
(define-syntax-rule (this-file)
  (path->string current-contract-region))

(define (join-test-exec testfun)
  (around
   (create-db)
   (define v1
     (open-view db-path "users"))
   (define v2
     (open-view db-path "posts"))
   (testfun v1 v2)
   (cleanup-db)))

(define (fetch-rows v)
  (cdr (fetch v)))

(define (check-rows v expected)
  (check-equal? (fetch-rows v) expected))

(define-test-suite
  shill-db-tests
  (test-suite
   "Join post tests"
   (test-pass
    "Join group post applies function"
    (join-test-exec
     (λ (v1 v2)
       (define/contract (f x y)
         (->j ([X #:post (λ (v) (select v "id"))])
              [(view/c +join +select +fetch) #:groups X]
              [(view/c +join +select +fetch) #:groups X]
              any)
         (join x y))
       (begin
         (insert v1 "id, name" (list 1 "Ezra"))
         (insert v2 "userid" (list 1))
         (check-rows (f v1 v2) '((1))))))))
  (test-suite
   "Join derive tests"
   (test-pass
    "Join group derive works for single layer case: derive a new privilege"
    (join-test-exec
     (λ (v1 v2)
       (define/contract (f x y)
         (->j ([X #:with (view/c +where)])
              [(view/c +join) #:groups X]
              [(view/c +join) #:groups X]
              any)
         (where (join x y) "id < 10"))
       (f v1 v2))))
   (test-contract-fail
    "Join group derive works for single layer case: lose a privilege"
    (join-test-exec
     (λ (v1 v2)
       (define/contract (f x y)
         (->j ([X #:with (view/c)])
              [(view/c +join +where) #:groups X]
              [(view/c +join) #:groups X]
              any)
         (where (join x y) "id < 10"))
       (f v1 v2)))
    "(function f)"))
  (test-pass
   "Join group derive works for nested case: both layers allow"
   (join-test-exec
    (λ (v1 v2)
      (define/contract (f x y)
        (->j ([X #:with (view/c +where)])
             [(view/c +join) #:groups X]
             [(view/c +join) #:groups X]
             any)
        (where (join x y) "id < 10"))
      (define/contract v1/c (view/c +join +where) v1)
      (f v1/c v2))))
  (test-contract-fail
   "Join group derive works for nested case: ->j layer allows"
   (join-test-exec
    (λ (v1 v2)
      (define/contract (f x y)
        (->j ([X #:with (view/c)])
             [(view/c +join) #:groups X]
             [(view/c +join) #:groups X]
             any)
        (where (join x y) "id < 10"))
      (define/contract v1/c (view/c +join +where) v1)
      (f v1/c v2)))
   "(function f)")
  (test-contract-fail
   "Join group derive works for nested case: inner layer allows"
   (join-test-exec
    (λ (v1 v2)
      (define/contract (f x y)
        (->j ([X #:with (view/c +where)])
             [(view/c +join) #:groups X]
             [(view/c +join) #:groups X]
             any)
        (where (join x y) "id < 10"))
      (define/contract v1/c (view/c +join) v1)
      (f v1/c v2)))
   (this-file)))

(run-tests shill-db-tests)