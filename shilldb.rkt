#lang racket

(require "db_api_impl.rkt")

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]
                    [select #:mutable]))

(define (build-view view)
  (define (fetch v) (fetch-impl (shill-view-view v)))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))
  (define (select v c) (build-view (select-impl (shill-view-view v) c)))

  (shill-view view fetch where select))

(define (mutator-redirect-proc i v) v)

(struct view-proxy (full-details param)
  #:property prop:contract
  (build-contract-property
   #:name
   (λ (ctc) 'view/c)
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
         (define fetch/c (make-fetch/c (assoc "fetch" full-details)))
         (define where/c (make-where/c (assoc "where" full-details)))
         (define select/c (make-select/c (assoc "select" full-details)))
         (unless (contract-first-order-passes? ctc val)
           (raise-blame-error blame val '(expected "a view" given "~e") val))
         ;(define old-rights (and (rights? val) (get-rights val)))
         ;(define rights (check-compatible node-
         (impersonate-struct val
                             shill-view-fetch (redirect-proc fetch/c)
                             set-shill-view-fetch! mutator-redirect-proc
                             shill-view-where (redirect-proc where/c)
                             set-shill-view-where! mutator-redirect-proc
                             shill-view-select (redirect-proc select/c)
                             set-shill-view-select! mutator-redirect-proc))))))

(define (make-fetch/c details)
  (define (fetch-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (f)
         (λ (v)
           (f (pre v)))))))
  (cond [(= (length details) 2)
         (->* (shill-view?) #:pre (second details) any)]
        [else
         (and/c (->* (shill-view?) #:pre (second details) any) (fetch-pre/c (third details)))]))

(define (make-where/c details)
  (define (where-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (f)
         (λ (v w)
           (f (pre v) w))))))
  (define dl (length details))
  (cond [(= dl 2)
         (->* (shill-view? string?) #:pre (second details) any)]
        [(= dl 3)
         (and/c (->* (shill-view? string?) #:pre (second details) any) (where-pre/c (third details)))]
        [(= dl 4)
         (and/c (->* (shill-view? (and/c string? (fourth details))) #:pre (second details) any) (where-pre/c (third details)))]))

(define (make-select/c details)
  (define (select-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (f)
         (λ (v c)
           (f (pre v) c))))))
  (define dl (length details))
  (cond [(= dl 2)
         (->* (shill-view? string?) #:pre (second details) any)]
        [(= dl 3)
         (and/c (->* (shill-view? string?) #:pre (second details) any) (select-pre/c (third details)))]
        [(= dl 4)
         (and/c (->* (shill-view? (and/c string? (fourth details))) #:pre (second details) any) (select-pre/c (third details)))]))

(define (view/c
         #:fetch [f (list "fetch" #f)]
         #:where [w (list "where" #f)]
         #:select [s (list "select" #f)])
  (view-proxy (list f w s) #f))

(define (fetch view) ((shill-view-fetch view) view))

(define (where view w) ((shill-view-where view) view w))

(define (select view c) ((shill-view-select view) view c))

(define (open-view filename table)
  (build-view (make-view-impl filename table)))

(module+ test
  (define/contract v1 (view/c #:fetch (list "fetch" #t (λ (v) (where v "a < 10")))
                              #:where (list "where" #t values (λ (w) (eq? w "a < 10")))) (open-view "test.db" "test"))
  (define/contract v2 (view/c #:fetch (list "fetch" (λ (v) (where v "b < 80")))) v1)
  (fetch v1))
