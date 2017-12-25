#lang racket

(require rackunit)

(provide check-fail)

(define-syntax-rule (check-fail expr error-msg)
  (check-exn (λ (exn)
               (and
                (exn:fail? exn)
                (regexp-match?
                 (regexp-quote error-msg)
                 (exn-message exn))))
             (λ () expr)))