#lang racket

(require "api/db_api_impl.rkt"
         "shilldb-utils.rkt"
         (only-in "api/sql_parse.rkt"
                  parse-where)
         (only-in "api/util.rkt"
                  hash-union
                  view-get-fks
                  view-get-type-map
                  view-get-tname-for-col
                  ast
                  fk-constraint
                  [cond condexp]
                  atom))
         
(provide
 view-proxy
 make-join-group
 shill-view?
 view/c
 where
 join
 select
 fetch
 update
 insert
 delete
 open-view)

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]
                    [select #:mutable]
                    [update #:mutable]
                    [insert #:mutable]
                    [delete #:mutable]
                    [join-constraint #:mutable #:auto])
  #:auto-value values)

(define (build-view view)
  (define (fetch v pre) (fetch-impl (shill-view-view (pre v))))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))
  (define (select v c) (build-view (select-impl (shill-view-view v) c)))
  (define (update v query pre [where ""]) (update-impl (shill-view-view (pre v)) query where))
  (define (insert v cols values pre) (insert-impl (shill-view-view (pre v)) cols values))
  (define (delete v pre) (delete-impl (shill-view-view (pre v))))
  ;(define (get-join-details pre-fun post-fun out-ctc-fun) (values pre-fun post-fun out-ctc-fun))

  (shill-view view fetch where select update insert delete))


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
         (define fetch/c (make-fetch/c val ctc (list-assoc "fetch" full-details)))
         (define where/c (make-where/c (list-assoc "where" full-details) full-details))
         (define select/c (make-select/c (list-assoc "select" full-details) full-details))
         (define update/c (make-update/c val ctc (list-assoc "update" full-details)))
         (define delete/c (make-delete/c val ctc (list-assoc "delete" full-details)))
         (define insert/c (make-insert/c val ctc (list-assoc "insert" full-details)))
         (define join/c (make-join/c (list-assoc "join" full-details)))
         ;(define get-join-details/c (make-get-join-details/c ctc (list-assoc "join" full-details)))
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
                             shill-view-delete (redirect-proc delete/c)
                             set-shill-view-delete! mutator-redirect-proc
                             shill-view-insert (redirect-proc insert/c)
                             set-shill-view-insert! mutator-redirect-proc
                             shill-view-join-constraint (redirect-proc join/c)
                             set-shill-view-join-constraint! mutator-redirect-proc                            
                             impersonator-prop:contracted ctc))))))

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

(define (make-join/c details)
  (enhance-blame/c
   (cond [(= (length details) 2)
          (->* (any/c) #:pre (second details) any)])
   "join"))

#|(define (make-get-join-details/c ctc details)
  (define (compose-pres p1 p2)
    (λ (v1 v2 jc)
      (and (p1 v1 v2 jc) (p2 v1 v2 jc))))
  (define (compose-posts inner outer)
    (λ (v1 v2 jc)
      (let ([p1 (inner v1 v2 jc)]
            [p2 (outer v1 v2 jc)])
        (λ (v) (p2 (p1 v))))))
  (define (compose-ctcs cf1 cf2)
    (λ (v1 v2 jc)
      (and/c (cf1 v1 v2 jc) (cf2 v1 v2 jc))))
  (define (join-details/c pre post out-ctc)
    (make-contract
     #:projection
     (λ (b)
       (λ (gjd)
         (λ (new-pre new-post new-out-ctc)
           (gjd (compose-pres pre new-pre)
                (compose-posts post new-post)
                (compose-ctcs out-ctc new-out-ctc)))))))
  (define dl (length details))
  (define (const-three-arg v) (λ (x y z) v))
  (cond [(= dl 3)
         (join-details/c (third details) (const-three-arg values) (const-three-arg any/c))]
        [(= dl 4)
         (join-details/c (third details) (fourth details) (const-three-arg any/c))]
        [(= dl 5)
         (join-details/c (third details) (fourth details) (fifth details))]
        [else
         (join-details/c (const-three-arg #t) (const-three-arg values) (const-three-arg any/c))]))|#

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

(define (make-delete/c view ctc details)
  (define (delete-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (d)
         (λ (v p)
           (d view (λ (v) (p (with-contract b #:result ctc (pre v))))))))))
  (enhance-blame/c
   (cond [(= (length details) 2)
          (->* (shill-view? procedure?) #:pre (second details) any)]
         [else
          (and/c (->* (shill-view? procedure?) #:pre (second details) any) (delete-pre/c (third details)))])
  "delete"))


(define (make-insert/c view ctc details)
  (define (insert-pre/c pre)
    (make-contract
     #:projection
     (λ (b)
       (λ (i)
         (λ (v c vals p)
           (i view c vals (λ (v) (p (with-contract b #:result ctc (pre v))))))))))
  (enhance-blame/c
   (cond [(= (length details) 2)
          (->* (shill-view? string? list? procedure?)
               #:pre (second details) any)]
         [else
          (and/c (->* (shill-view? string? list? procedure?)
                      #:pre (second details) any) (insert-pre/c (third details)))])
  "insert"))


#| (define (make-pre-join/c view ctc details)
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
    "join"))|#
   

(define (view/c
         #:fetch [f (list "fetch" #f)]
         #:where [w (list "where" #f)]
         #:select [s (list "select" #f)]
         #:update [u (list "update" #f)]
         #:insert [i (list "insert" #f)]
         #:delete [d (list "delete" #f)]
         #:join [j (list "join" #f)])
  (view-proxy (list f w s u i d j) #f))

(define (fetch view) ((shill-view-fetch view) view values))

(define (where view w) ((shill-view-where view) view w))

(define (select view c) ((shill-view-select view) view c))

(define (update v query [where ""]) ((shill-view-update v) v query values where))

(define (insert v cols vals) ((shill-view-insert v) v cols vals values))

(define (delete v) ((shill-view-delete v) v values))

#|(define (join v1 v2 jcond)
  (define id-transform (λ (v1 v2 jc) values))
  (define (get-join-details v) ((shill-view-get-join-details v) (λ (v1 v2 jc) #t) (λ (v1 v2 jc) values) (λ (v1 v2 jc) any/c)))
  (define (get-pre-post-out v)
    (match (map (λ (f) (apply f (list v1 v2 jcond))) (call-with-values (λ () (get-join-details v)) list))
      [(list pre post out) (values pre post out)]))
  (let-values ([(v1-pre v1-post v1-out/c) (get-pre-post-out v1)]
               [(v2-pre v2-post v2-out/c) (get-pre-post-out v2)])
    (define/contract (build-intermediate v1 v2)
      (-> shill-view? shill-view? (and/c v1-out/c v2-out/c))
      (build-view (join-impl (shill-view-view v1) (shill-view-view v2) jcond)))
    (if (and v1-pre v2-pre)
        (v2-post (v1-post (build-intermediate v1 v2)))
        #f))); FIXME|#

(define (join v1 v2 jcond)
  (define (real-join v1 v2 jcond)
    (build-view (join-impl (shill-view-view v1) (shill-view-view v2) jcond)))
  (((compose (shill-view-join-constraint v1)
             (shill-view-join-constraint v2))
    real-join)
   v1
   v2
   jcond))

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

(define (make-join-group)
  (define store (box '()))

  (struct valid-arg/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (if (memf (λ (v) (equal? (car v) val)) (unbox store))
               val
               (raise-blame-error
                blame
                val
                "not one of the expected join arguments")))))))

  (struct record-arg/c (post-tf derive/c)
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (define post-tf (record-arg/c-post-tf ctc))
           (define derive/c (record-arg/c-derive/c ctc))
           (set-box! store (cons (list val post-tf derive/c) (unbox store)))
           val)))))

  (struct apply-post/c (v1 v2 jcond blame)
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (define-values (v1 v2 jcond real-blame)
             (values
              (apply-post/c-v1 ctc)
              (apply-post/c-v2 ctc)
              (apply-post/c-jcond ctc)
              (apply-post/c-blame ctc)))
           ; stick this stuff in a struct so it's nicer
           (define (get v)
             (first (memf (λ (x) (equal? (car x) v)) (unbox store))))

           (define details1 (get v1))
           (define details2 (get v2))
           
           ((compose (cadr details2) (cadr details1))
            (((contract-projection (caddr details2)) real-blame)
             (((contract-projection (caddr details1)) real-blame) val))))))))
  
  (struct constraint-args/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (define new-constraint/c
           (->i ([v1 (valid-arg/c)]
                 [v2 (valid-arg/c)]
                 [jcond any/c])
                [res (v1 v2 jcond) (apply-post/c v1 v2 jcond blame)]))
         (define new-constraint ((contract-projection new-constraint/c) blame))
         (define (redirect proc) (λ (s v) proc))
         (λ (val)
           ; No constraint on self, just use `values`
           (set-box! store (cons (list val values any/c) (unbox store)))
           (impersonate-struct val                              
                               shill-view-join-constraint (λ (s v) (compose new-constraint v))
                               set-shill-view-join-constraint! mutator-redirect-proc
                               impersonator-prop:contracted (and/c (value-contract val) ctc)))))))
  (values (constraint-args/c) record-arg/c))

(module+ test
  (define example/c
    (let-values ([(XO X) (make-join-group)]
                 [(YO Y) (make-join-group)])
      (->
       (and/c shill-view? XO (Y (λ (v) (where v "a < 10")) (view-proxy (list (list "fetch" #t) (list "where" #t)) #f)))
       (and/c shill-view? YO (X (λ (v) (where v "a < 10")) (view-proxy (list (list "fetch" #t) (list "where" #t)) #f)))
       any)))

  (define/contract (f x y)
    example/c
    (fetch (join x y "")))

  (f (open-view "test.db" "students") (open-view "test.db" "test")))