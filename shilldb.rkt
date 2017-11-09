#lang racket

(require "db_api_impl.rkt")

(provide
 view/c
 view-pair/c
 fetch
 update
 where
 join
 pjoin
 select
 open-view
 build-view-pair)

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]
                    [select #:mutable]
                    [update #:mutable]
                    [pre-join #:mutable]
                    [view-contract #:mutable]))

(define (build-view view)
  (define (fetch v pre) (fetch-impl (shill-view-view (pre v))))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))
  (define (select v c) (build-view (select-impl (shill-view-view v) c)))
  (define (update v query pre [where ""]) (update-impl (shill-view-view (pre v)) query where))
  (define (pre-join v pre) (pre v))
  (define (view-contract ctc) ctc)

  (shill-view view fetch where select update pre-join view-contract))

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
         (define update/c (make-update/c val ctc (assoc "update" full-details)))
         (define pre-join/c (make-pre-join/c val ctc (assoc "join" full-details)))
         (define view-contract/c (make-view-contract/c ctc (assoc "join" full-details)))
         (unless (contract-first-order-passes? ctc val)
           (raise-blame-error blame val '(expected "a view" given "~e") val))
         (impersonate-struct val
                             shill-view-fetch (redirect-proc fetch/c)
                             set-shill-view-fetch! mutator-redirect-proc
                             shill-view-where (redirect-proc where/c)
                             set-shill-view-where! mutator-redirect-proc
                             shill-view-select (redirect-proc select/c)
                             set-shill-view-select! mutator-redirect-proc
                             shill-view-update (redirect-proc update/c)
                             set-shill-view-update! mutator-redirect-proc
                             shill-view-pre-join (redirect-proc pre-join/c)
                             set-shill-view-pre-join! mutator-redirect-proc
                             shill-view-view-contract (redirect-proc view-contract/c)
                             set-shill-view-view-contract! mutator-redirect-proc))))))

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

(define (make-view-contract/c ctc details)
  (define (view-contract/c con)
    (make-contract
     #:projection
     (λ (b)
       (λ (vc)
         (λ (c)
           (vc (and/c con c)))))))
  (cond [(= (length details) 4)
         (view-contract/c (fourth details))]
        [else
         (view-contract/c ctc)]))

(define (make-where/c details full-details)
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? string?) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 3)
          (->* (shill-view? (and/c string? (third details))) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 4)
          (->* (shill-view? (and/c string? (third details))) #:pre (second details) (fourth details))])
  "where"))
  
(define (make-select/c details full-details)
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? string?) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 3)
          (->* (shill-view? (and/c string? (third details))) #:pre (second details) (view-proxy full-details #f))]
         [(= dl 4)
          (->* (shill-view? (and/c string? (third details))) #:pre (second details) (fourth details))])
  "select"))

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
         [(= dl 3)
          (->* (shill-view? (and/c string? (third details)) procedure?) (string?) #:pre (second details) any)]
         [(= dl 4)
          (->* (shill-view? (and/c string? (third details)) procedure?) ((and/c string? (fourth details))) #:pre (second details) any)]
         [(= dl 5)
          (and/c (->* (shill-view? (and/c string? (third details)) procedure?) ((and/c string? (fourth details))) #:pre (second details) any) (update-pre/c (fifth details)))])
  "update"))

 (define (make-pre-join/c view ctc details)
   (define (pre-join-pre/c pre)
     (make-contract
      #:projection
      (λ (b)
        (λ (pj)
          (λ (v p)
            (pj view (λ (v) (p (with-contract b #:result ctc (pre v))))))))))
   (define dl (length details))
   (enhance-blame/c
    (cond [(= dl 2)
           (->* (shill-view? procedure?) #:pre (second details) any)]
          [(= dl 3)
           (and/c (->* (shill-view? procedure?) #:pre (second details) any) (pre-join-pre/c (third details)))]
          [(= dl 4)
           (and/c (->* (shill-view? procedure?) #:pre (second details) (fourth details)) (pre-join-pre/c (third details)))])
    "join"))
   

(struct view-pair (v1
                   v2
                   [join #:mutable]))

(define (build-view-pair v1 v2)
  (define (pre-join v) ((shill-view-pre-join v) v values))
  (define (join v1 v2 jcond post)
    (define (get-view-contract v) ((shill-view-view-contract v) any/c))
    (define v1/c (get-view-contract v1))
    (define v2/c (get-view-contract v2))
    (define/contract join-res (and/c v1/c v2/c) (build-view (join-impl (shill-view-view (pre-join v1)) (shill-view-view (pre-join v2)) jcond)))
    (post join-res))

  (view-pair v1 v2 join))

(struct view-pair-proxy (full-details param)
  #:property prop:contract
  (build-contract-property
   #:name
   (λ (ctc) 'view-pair/c)
   #:first-order
   (λ (ctc)
     (λ (val)
       (view-pair? val)))
   #:projection
   (λ (ctc)
     (define full-details (view-pair-proxy-full-details ctc))
     (λ (blame)
       (define (redirect-proc accessor-contract)
         (λ (vp field-value)
           (((contract-projection accessor-contract) blame) field-value)))
       (λ (val)
         (define join/c (make-join/c val ctc (assoc "join" full-details)))
         (unless (contract-first-order-passes? ctc val)
           (raise-blame-error blame val '(expected "a view pair" given "~e") val))
         (impersonate-struct val
                             view-pair-join (redirect-proc join/c)
                             set-view-pair-join! mutator-redirect-proc))))))

(define (make-join/c vp ctc details)
  (define (join-post/c post)
    (make-contract
     #:projection
     (λ (b)
       (λ (j)
         (λ (v1 v2 jcond p)
           (j v1 v2 jcond (λ (v) (p (post v)))))))))
  (define dl (length details))
  (enhance-blame/c
   (cond [(= dl 2)
          (->* (shill-view? shill-view? string? procedure?) #:pre (second details) shill-view?)]
         [(= dl 3)
          (and/c (join-post/c (third details)) (->* (shill-view? shill-view? string? procedure?) #:pre (second details) shill-view?))])
   "join"))

(define (view/c
         #:fetch [f (list "fetch" #f)]
         #:where [w (list "where" #f)]
         #:select [s (list "select" #f)]
         #:update [u (list "update" #f)]
         #:join [j (list "join" #f)])
  (view-proxy (list f w s u j) #f))

(define (view-pair/c
         #:join [j (list "join" #f)])
  (view-pair-proxy (list j) #f))

(define (fetch view) ((shill-view-fetch view) view values))

(define (where view w) ((shill-view-where view) view w))

(define (select view c) ((shill-view-select view) view c))

(define (update v query [where ""]) ((shill-view-update v) v query values where))

(define (join v1 v2 jcond)
  (define/contract vp (view-pair/c #:join (list "join" #t)) (build-view-pair v1 v2))
  (pjoin vp jcond))
  
(define (pjoin vp jcond) ((view-pair-join vp) (view-pair-v1 vp) (view-pair-v2 vp) jcond values))

(define (open-view filename table)
  (build-view (make-view-impl filename table)))

(module+ test
  (define/contract v1 (view/c #:fetch (list "fetch" #t)
                              #:select (list "select" #t)
                              #:update (list "update" #t)
                              #:where (list "where" #t any/c (view/c #:fetch (list "fetch" #t)))
                              #:join (list "join" #t))
    (open-view "test.db" "test"))
  (define/contract v2 (view/c #:join (list "join" #t (λ (v) (where v "a < 2"))) #:fetch (list "fetch" #t (λ (v) (select v "b")))) v1))
