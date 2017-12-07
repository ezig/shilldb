#lang racket

(require "db_api_impl.rkt"
         (only-in "sql_parse.rkt"
                  parse-where)
         (only-in "util.rkt"
                  hash-union
                  view-get-fks
                  view-get-type-map
                  view-get-tname-for-col
                  ast
                  fk-constraint
                  [cond condexp]
                  atom))
         
(provide
 view/c
 fetch
 update
 where
 join
 select
 open-view)

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]
                    [select #:mutable]
                    [update #:mutable]                  
                    [get-join-details #:mutable]))

(define (build-view view)
  (define (fetch v pre) (fetch-impl (shill-view-view (pre v))))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))
  (define (select v c) (build-view (select-impl (shill-view-view v) c)))
  (define (update v query pre [where ""]) (update-impl (shill-view-view (pre v)) query where))
  (define (get-join-details this-pre that-pre post out-ctc) (values this-pre that-pre post out-ctc))

  (shill-view view fetch where select update get-join-details))

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
         (define get-join-details/c (make-get-join-details/c ctc (assoc "join" full-details)))
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
                             shill-view-get-join-details (redirect-proc get-join-details/c)
                             set-shill-view-get-join-details! mutator-redirect-proc
                             impersonator-prop:contracted ctc))))))

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

(define (make-get-join-details/c ctc details)
  (define (compose-transforms inner outer)
    (λ (v1 v2 jc)
      (let ([innerf (inner v1 v2 jc)])
        (if innerf
            (let ([outerf (outer v1 v2 jc)])
              (if outerf
                  (λ (v) (outerf (innerf v)))
                  #f))
            #f))))
  (define (join-details/c this-pre that-pre post out-ctc)
    (make-contract
     #:projection
     (λ (b)
       (λ (gjd)
         (λ (new-this-pre new-that-pre new-post new-out-ctc)
           (gjd (compose-transforms this-pre new-this-pre)
                (compose-transforms that-pre new-that-pre)
                (compose-transforms post new-post)
                (and/c new-out-ctc out-ctc)))))))
  (define dl (length details))
  (define id-transform (λ (v1 v2 jc) values))
  (cond [(= dl 3)
         (join-details/c (third details) id-transform id-transform any/c)]
        [(= dl 4)
         (join-details/c (third details) (fourth details) id-transform any/c)]
        [(= dl 5)
         (join-details/c (third details) (fourth details) (fifth details) any/c)]
        [(= dl 6)
         (join-details/c (third details) (fourth details) (fifth details) (sixth details))]
        [else
         (join-details/c id-transform id-transform id-transform any/c)]))

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
          [(>= dl 3)
           (and/c (->* (shill-view? procedure?) #:pre (second details) any) (pre-join-pre/c (third details)))])
    "join"))
   

(define (view/c
         #:fetch [f (list "fetch" #f)]
         #:where [w (list "where" #f)]
         #:select [s (list "select" #f)]
         #:update [u (list "update" #f)]
         #:join [j (list "join" #f)])
  (view-proxy (list f w s u j) #f))

(define (fetch view) ((shill-view-fetch view) view values))

(define (where view w) ((shill-view-where view) view w))

(define (select view c) ((shill-view-select view) view c))

(define (update v query [where ""]) ((shill-view-update v) v query values where))

(define (join v1 v2 jcond)
  (define id-transform (λ (v1 v2 jc) values))
  (define (get-join-details v) ((shill-view-get-join-details v) id-transform id-transform id-transform any/c))
  (define-values (v1-this-pre-f v1-that-pre-f v1-post-f v1-out/c) (get-join-details v1))
  (define-values (v2-this-pre-f v2-that-pre-f v2-post-f v2-out/c) (get-join-details v2))
  (define (apply-pre v this that)
    (if this
        (if that
            (that (this v))
            #f)
        #f))
  (let* ([v1-this-pre (v1-this-pre-f v1 v2 jcond)]
         [v1-that-pre (v1-that-pre-f v1 v2 jcond)]
         [v1-post (v1-post-f v1 v2 jcond)]
         [v2-this-pre (v2-this-pre-f v1 v2 jcond)]
         [v2-that-pre (v2-that-pre-f v1 v2 jcond)]
         [v2-post (v2-post-f v1 v2 jcond)]
         [v1-pre (apply-pre v1 v1-this-pre v2-that-pre)]
         [v2-pre (apply-pre v2 v2-this-pre v1-that-pre)])
    (define/contract intermediate (and/c v1-out/c v2-out/c) (build-view (join-impl (shill-view-view v1-pre) (shill-view-view v2-pre) jcond)))
    (if (and v1-pre v2-pre)
          (v2-post (v1-post intermediate))
        #f)))

(define (open-view filename table)
  (build-view (make-view-impl filename table)))

(define (fk-pred svthis svthat jcond)
  (let* ([vthis (shill-view-view svthis)]
         [vthat (shill-view-view svthat)]
         [tm (hash-union (view-get-type-map vthis) (view-get-type-map vthat))])
    (match (parse-where jcond tm)
      [(ast 'where (condexp '= (atom _ #t val1) (atom _ #t val2)))
       (if (and (hash-has-key? tm val1) (hash-has-key? tm val2))
           (let ([fks (view-get-fks vthat)])
             (define (validate-fks ref-col fk-col)
               (integer? (index-of fks (fk-constraint (view-get-tname-for-col vthis ref-col) ref-col fk-col))))
             (or (validate-fks val1 val2) (validate-fks val2 val1)))
           #f)]
      [_ #f])))

(module+ test
  ;(define/contract v1 (view/c #:fetch (list "fetch" #t) #:join (list "join" #t values any/c (λ (v) (displayln v)))) (open-view "test.db" "test"))
  ;(define va (open-view "test.db" "artist"))
  ;(define vt (open-view "test.db" "track"))
  ;(fk-pred va vt "trackartist = trackartist"))


; (view/c (fetch/p (where "x < 10"))

  ;  (fetch (join v1 v1 "")))
  (define top (view/c #:fetch (list "fetch" #t)
                              #:select (list "select" #t)
                              #:update (list "update" #t)
                              #:where (list "where" #t)
                              #:join (list "join" #t)))
  (define/contract v1 (view/c #:fetch (list "fetch" #t)
                              #:select (list "select" #t)
                              #:update (list "update" #t)
                              #:where (list "where" #t)
                              #:join (list "join" #t (λ (x y z) #f) (λ (x y z) (λ (v) (where v "name = 'Ezra'")))))
                              ;#:join (list "join" #t (λ (v) v) (λ (p) (displayln p))))
    (open-view "test.db" "test"))
  (define/contract v2 top (open-view "test.db" "students"))
  ;(define/contract v2 (view/c #:fetch (list "fetch" #t (λ (v) (select v "b"))) #:where (list "where" #t)) v1)
  ;(define/contract x (value-contract v2) (open-view "test.db" "test"))
  ;(define/contract v2 (view/c #:join (list "join" #t (λ (v) (where v "a < 2"))) #:fetch (list "fetch" #t (λ (v) (select v "b")))) v1)
  ;(fetch v2))
  (fetch (join v1 v2 "")))
  ;(fetch (join v2 v1 "trackartist = artistid")))
