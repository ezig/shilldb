#lang racket

(require rackunit
         db)

(provide test-fail
         test-pass
         test-contract-fail
         check-fail
         create-db
         create-schema
         cleanup-db
         db-path
         script)

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


(define-syntax-rule (test-pass test-name expr) 
  (test-case test-name (check-pass expr)))

(define-syntax-rule (check-pass expr)
  (check-not-exn (λ () expr)))

(define-syntax-rule (test-fail test-name expr error-msg) 
  (test-case test-name (check-fail expr error-msg)))

(define-syntax-rule (check-fail expr error-msg)
  (check-exn (λ (exn)
               (and
                (exn:fail? exn)
                (regexp-match?
                 (regexp-quote error-msg)
                 (exn-message exn))))
             (λ () expr)))

(define-syntax test-contract-fail
  (syntax-rules ()
    [(test-contract-fail test-name expr error-msg)
     (test-case test-name (check-contract-fail expr error-msg))]
    [(test-contract-fail test-name  expr error-msg extra-msg)
     (test-case test-name 
                (check-contract-fail expr error-msg extra-msg))]))


(define-syntax check-contract-fail
  (syntax-rules ()
    [(check-contract-fail expr error-msg)
     (check-exn (λ (exn)
                  (and (exn:fail? exn)
                       (has-proper-blame? error-msg (exn-message exn))))
                (λ () expr))]
    [(check-contract-fail expr error-msg extra-msg)
     (check-exn (λ (exn)
                  (and (exn:fail? exn)
                       (has-proper-blame? error-msg (exn-message exn) extra-msg)))
                (λ () expr))]))

(define (has-proper-blame? blame msg [extra ""])
  (define reg
    (cond
      [(string? blame) (string-append "blaming: " (regexp-quote blame))]
      [else #f]))
  (define extra-reg (regexp-quote extra))
  (and reg (regexp-match? reg msg) (regexp-match? extra-reg msg)))

(define-syntax-rule (script e ...)
  (parameterize ([current-namespace (make-base-namespace)])
    (namespace-require '(for-syntax racket/base))
    (eval 'e) ...))
