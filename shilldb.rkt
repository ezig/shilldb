#lang racket

(require "db_api_impl.rkt")

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]))

(define (build-view view)
  (define (fetch v) (fetch-impl (shill-view-view v)))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))

  (shill-view view fetch where))

(define (mutator-redirect-proc i v) v)

(struct view-proxy (full-details param)
  #:property prop:contract
  (build-contract-property
   #:name
   (λ (ctc) 'file/c)
   #:first-order
   (λ (ctc)
     (λ (val)
       (shill-view? val)))
   #:projection
   (λ (ctc)
     (define full-details (view-proxy-full-details ctc))
     (λ (blame)
       (define (redirect-proc accessor-contract)
         (λ (view field-value)
           (((contract-projection accessor-contract) blame) field-value)))
       (λ (val)
         (define fetch/c (make-fetch/c val (assoc "fetch" full-details)))
         (define where/c (make-where/c val (assoc "where" full-details)))
         (unless (contract-first-order-passes? ctc val)
           (raise-blame-error blame val '(expected "a view" given "~e") val))
         ;(define old-rights (and (rights? val) (get-rights val)))
         ;(define rights (check-compatible node-
         (impersonate-struct val
                             shill-view-fetch (redirect-proc fetch/c)
                             set-shill-view-fetch! mutator-redirect-proc
                             shill-view-where (redirect-proc where/c)
                             set-shill-view-where! mutator-redirect-proc))))))

(define (make-fetch/c view details)
  (define (fetch-pre/c view pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (f)
         (λ (v)
           (f (pre view)))))))
  (cond [(list? details)
         (fetch-pre/c view (second details))]))

(define (make-where/c view details)
  (define (where-pre/c view pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (f)
         (λ (v w)
           (f (pre view) w))))))
  (cond [(list? details)
         (where-pre/c view (second details))]))

(define (view/c
         #:fetch [f (list "fetch" (λ (x) x))]
         #:where [w (list "where" (λ (x) x))])
  (view-proxy (list f w) #f))

(define (fetch view) ((shill-view-fetch view) view))

(define (where view w) ((shill-view-where view) view w))

(define (open-view filename table)
  (build-view (make-view-impl filename table)))

(module+ test
  (define/contract v1 (view/c #:fetch (list "where" (λ (v) (where v "a < 10")))) (open-view "test.db" "test"))
  (define/contract v2 (view/c #:fetch (list "fetch" (λ (v) (where v "b < 80")))) v1)
  ;(fetch (where (open-view "test.db" "test") "a < 5"))
  (fetch (where v1 "b < 80")))
