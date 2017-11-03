#lang racket

(require "db_api_impl.rkt")

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]
                    [select #:mutable]
                    [join #:mutable]
                    [update #:mutable]))

(define (build-view view)
  (define (fetch v pre) (fetch-impl (shill-view-view (pre v))))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))
  (define (select v c) (build-view (select-impl (shill-view-view v) c)))
  (define (join v1 v2 jcond) (build-view (join-impl (shill-view-view v1) (shill-view-view v2) jcond)))
  (define (update v query pre [where ""]) (update-impl (shill-view-view (pre v)) query where))

  (shill-view view fetch where select join update))

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
         (define fetch/c (make-fetch/c val ctc (assoc "fetch" full-details)))
         (define where/c (make-where/c (assoc "where" full-details) full-details))
         (define select/c (make-select/c (assoc "select" full-details) full-details))
         (define join/c (make-join/c (assoc "join" full-details) full-details))
         (define update/c (make-update/c val ctc (assoc "update" full-details)))
         (unless (contract-first-order-passes? ctc val)
           (raise-blame-error blame val '(expected "a view" given "~e") val))
         (impersonate-struct val
                             shill-view-fetch (redirect-proc fetch/c)
                             set-shill-view-fetch! mutator-redirect-proc
                             shill-view-where (redirect-proc where/c)
                             set-shill-view-where! mutator-redirect-proc
                             shill-view-select (redirect-proc select/c)
                             set-shill-view-select! mutator-redirect-proc
                             shill-view-join (redirect-proc join/c)
                             set-shill-view-join! mutator-redirect-proc
                             shill-view-update (redirect-proc update/c)
                             set-shill-view-update! mutator-redirect-proc))))))

(struct enhance-blame/c (ctc msg)
  #:property prop:contract
  (build-contract-property
   #:projection
   (λ (ctc)
     (define inner-ctc (enhance-blame/c-ctc ctc))
     (define msg (enhance-blame/c-msg ctc))
     (λ (blame)
       (define new-blame 
         (blame-add-context 
          blame 
          (string-append msg " (insufficient privileges for " msg "!) in")))
       (λ (val)
         (((contract-projection inner-ctc) new-blame) val))))))

(define (make-fetch/c view ctc details)
  (define (fetch-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (f)
         (λ (v p)
           (f view (λ (v) (p (with-contract b #:result ctc (pre v))))))))))
  (enhance-blame/c
   (cond [(= (length details) 2)
          (->* (shill-view? procedure?) #:pre (second details) any)]
         [else
          (and/c (->* (shill-view? procedure?) #:pre (second details) any) (fetch-pre/c (third details)))])
  "fetch"))

(define (make-where/c details full-details)
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? string?) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 3)
          (->* (shill-view? (and/c string? (third details))) #:pre (second details) (view-proxy full-details #f))])
  "where"))
  
(define (make-select/c details full-details)
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? string?) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 3)
          (->* (shill-view? (and/c string? (third details))) #:pre (second details) (view-proxy full-details #f))])
  "select"))

(define (make-join/c details full-details)
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? shill-view? string?) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 3)
          (->* (shill-view? shill-view? (and/c string? (third details))) #:pre (second details) (view-proxy full-details #f))])
   "join"))

(define (make-update/c view ctc details)
  (define (update-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (u)
         (λ (v q p [w ""])
           (u view q (λ (v) (p (with-contract b #:result ctc (pre v)))) w))))))
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? string? procedure?) (string?) #:pre (second details) any)]
         [else
          (and/c (->* (shill-view? string? procedure?) (string?) #:pre (second details) any) (update-pre/c (third details)))])
  "update"))

(define (view/c
         #:fetch [f (list "fetch" #f)]
         #:where [w (list "where" #f)]
         #:select [s (list "select" #f)]
         #:join [j (list "join" #f)]
         #:update [u (list "update" #f)])
  (view-proxy (list f w s j u) #f))

(define (fetch view) ((shill-view-fetch view) view values))

(define (where view w) ((shill-view-where view) view w))

(define (select view c) ((shill-view-select view) view c))

(define (join v1 v2 jcond) ((shill-view-join v1) v1 v2 jcond))

(define (update v query [where ""]) ((shill-view-update v) v query values where))

(define (open-view filename table)
  (build-view (make-view-impl filename table)))

(module+ test
  (define/contract v1 (view/c #:fetch (list "fetch" #t)
                              #:select (list "select" #t)
                              #:update (list "update" #t)
                              #:where (list "where" #t))
    (open-view "test.db" "test"))
  (define/contract v2 (view/c #:fetch (list "fetch" #t (λ (v) (select v "b")))) v1)
  (define/contract v3 (view/c #:fetch (list "fetch" #t) #:join (list "join" #t)) (open-view "test.db" "test"))
  (define/contract (f w v) (->i ([w integer?] [v (w) (view/c #:update (list "update" #t (λ (v) (where v (format "a = ~a" w)))))]) [result any/c])
    (update v "b = b + 1"))
  (fetch v1)
  (f 5 v1)
  (fetch v1))
