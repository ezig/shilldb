#lang racket

(require "api/db_api_impl.rkt"
         "shilldb-utils.rkt"
         (only-in "api/sql_parse.rkt"
                  parse-where)
         "api/util.rkt")
         
(provide
 view-proxy
 make-join-group
 shill-view?
 view/c
 where
 join
 select
 mask
 aggregate
 fetch
 update
 insert
 delete
 open-view)

(struct shill-view (view
                    [fetch #:mutable]
                    [where #:mutable]
                    [select #:mutable]
                    [mask #:mutable]
                    [aggregate #:mutable]
                    [update #:mutable]
                    [insert #:mutable]
                    [delete #:mutable]
                    [join-derive #:mutable]
                    [join-constraint #:mutable #:auto])
  #:auto-value values)

(define (build-view view)
  (define (fetch v pre) (fetch-impl (shill-view-view (pre v))))
  (define (where v w) (build-view (where-impl (shill-view-view v) w)))
  (define (select v c) (build-view (select-impl (shill-view-view v) c)))
  (define (mask v c) (build-view (mask-impl (shill-view-view v) c)))
  (define (aggregate v c groupby having)
    (build-view (aggregate-impl (shill-view-view v) c #:groupby groupby #:having having)))
  (define (update v query pre [where ""]) (update-impl (shill-view-view (pre v)) query where))
  (define (insert v cols values pre) (insert-impl (shill-view-view (pre v)) cols values))
  (define (delete v pre) (delete-impl (shill-view-view (pre v))))
  (define (join-derive v1 v2 jcond) values)

  (shill-view view fetch where select mask aggregate update insert delete join-derive))


(struct view-proxy (full-details join-skip-layer)
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
     (define join-skip-layer (view-proxy-join-skip-layer ctc))
     (λ (blame)
       (define (redirect-proc accessor-contract)
         (λ (view field-value)
           (((contract-projection accessor-contract) blame) field-value)))
       (λ (val)
         (define fetch/c (make-fetch/c val ctc (list-assoc "fetch" full-details)))
         (define where/c (make-where/c (list-assoc "where" full-details) full-details))
         (define select/c (make-select/c (list-assoc "select" full-details) full-details))
         ; Just use the select privileges for mask
         (define mask/c (make-mask/c (list-assoc "select" full-details) full-details))
         (define aggregate/c (make-aggregate/c (list-assoc "aggregate" full-details) full-details))
         (define update/c (make-update/c val ctc (list-assoc "update" full-details)))
         (define delete/c (make-delete/c val ctc (list-assoc "delete" full-details)))
         (define insert/c (make-insert/c val ctc (list-assoc "insert" full-details)))
         (define join-constraint/c (make-join/c (list-assoc "join" full-details)))

         (define new-derive ((contract-projection ctc) blame))
         (define join-derive (λ (s v)
                               (if join-skip-layer
                                   v
                                   (λ (v1 v2 jcond)
                                     (compose new-derive (v v1 v2 jcond))))))
         
         (unless (contract-first-order-passes? ctc val)
           (raise-blame-error blame val '(expected "a view" given "~e") val))
         (impersonate-struct val
                             shill-view-fetch (redirect-proc fetch/c)
                             set-shill-view-fetch! mutator-redirect-proc
                             shill-view-where (redirect-proc where/c)
                             set-shill-view-where! mutator-redirect-proc
                             shill-view-select (redirect-proc select/c)
                             set-shill-view-select! mutator-redirect-proc
                             shill-view-mask (redirect-proc mask/c)
                             set-shill-view-mask! mutator-redirect-proc
                             shill-view-aggregate (redirect-proc aggregate/c)
                             set-shill-view-aggregate! mutator-redirect-proc
                             shill-view-update (redirect-proc update/c)
                             set-shill-view-update! mutator-redirect-proc
                             shill-view-delete (redirect-proc delete/c)
                             set-shill-view-delete! mutator-redirect-proc
                             shill-view-insert (redirect-proc insert/c)
                             set-shill-view-insert! mutator-redirect-proc
                             shill-view-join-constraint (redirect-proc join-constraint/c)
                             set-shill-view-join-constraint! mutator-redirect-proc
                             shill-view-join-derive join-derive
                             set-shill-view-join-derive! mutator-redirect-proc
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

(define (make-mask/c details full-details)
  (enhance-blame/c
   (cond [(= 2 (length details))
          (->* (shill-view? string?) #:pre (second details) (view-proxy full-details #f))])
  "mask"))

(define (make-aggregate/c details full-details)
  (enhance-blame/c
   (cond [(= 2 (length details))
          (->* (shill-view? string? (or/c boolean? string?) (or/c boolean? string?))
               #:pre (second details) (view-proxy full-details #f))])
  "aggregate"))

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

(define (mask view c) ((shill-view-mask view) view c))

(define (aggregate view c #:groupby [groupby #f] #:having [having #f])
  ((shill-view-aggregate view) view c groupby having))

(define (update v query [where ""]) ((shill-view-update v) v query values where))

(define (insert v cols vals) ((shill-view-insert v) v cols vals values))

(define (delete v) ((shill-view-delete v) v values))

(define (join v1 v2 [jcond ""])
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

  (struct store-entry (v post derive))
  
  (define (in-store? val)
    (memf (λ (e) (equal? (store-entry-v e) val)) (unbox store)))

  ; Only call if certain in-box? is not #f
  (define (get-store-entry v)
    (first (in-store? v)))

  (define (add-to-store entry)
    (set-box! store (cons entry (unbox store))))
  
  (struct valid-arg/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (if (in-store? val)
               val
               (raise-blame-error
                blame
                val
                "not one of the expected join arguments")))))))

  ; if no derive is given, this needs to get the join-derive off of the val I think
  (struct record-arg/c (post-tf derive/c)
    #:property prop:contract
    (build-contract-property
     #:first-order
     (λ (ctc)
       (λ (val)
         (not (in-store? val))))
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (unless (contract-first-order-passes? ctc val)
             (raise-blame-error blame
                                val
                                "two views appeared together in multiple groups with different modifiers"))

           (define arg-post-tf (record-arg/c-post-tf ctc))
           (define arg-derive/c (record-arg/c-derive/c ctc))

           (let ; If no post is given, use identity
               ([post-tf (if arg-post-tf
                              arg-post-tf
                              values)]
                ; Derive must be given
                 [derive/c (λ (v1 v2 jcond) arg-derive/c)])
             (add-to-store (store-entry val post-tf derive/c))
             val))))))

  (struct apply-post/c (v1 v2 jcond)
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (define-values (v1 v2 jcond)
             (values
              (apply-post/c-v1 ctc)
              (apply-post/c-v2 ctc)
              (apply-post/c-jcond ctc)))
           (define entry1 (get-store-entry v1))
           (define entry2 (get-store-entry v2))
           
           ((compose (store-entry-post entry2)
                     (store-entry-post entry1)
                     ((shill-view-join-derive v2) v1 v2 jcond)
                     ((shill-view-join-derive v1) v1 v2 jcond)) val))))))
  
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
                [res (v1 v2 jcond) (apply-post/c v1 v2 jcond)]))
         (define new-constraint ((contract-projection new-constraint/c) blame))

         (define new-derive
           (λ (s v)
             (λ (v1 v2 jcond)              
               (define entry1 (get-store-entry v1))
               (define entry2 (get-store-entry v2))

               (compose ((contract-projection
                          ((store-entry-derive entry2) v1 v2 jcond)) blame)
                        ((contract-projection
                          ((store-entry-derive entry1) v1 v2 jcond)) blame)
                        (v v1 v2 jcond)))))
         
         (define (redirect proc) (λ (s v) proc))
         (λ (val)
           ; No constraint on self, just use `values`
           (add-to-store (store-entry val values (λ (v1 v2 jcond) any/c)))
           (impersonate-struct val                              
                               shill-view-join-constraint (λ (s v) (compose new-constraint v))
                               set-shill-view-join-constraint! mutator-redirect-proc
                               shill-view-join-derive new-derive
                               set-shill-view-join-derive! mutator-redirect-proc
                               impersonator-prop:contracted (and/c (value-contract val) ctc)))))))
  (values (constraint-args/c) record-arg/c))

(module+ test
  (define example/c
    (let-values ([(g1 g2) (make-join-group)] [(g3 g4) (make-join-group)])
     (->
      (and/c g1 (and/c (g4 (view-proxy (list (list "join" #t) (list "fetch" #t)) #t) #f)))
      (and/c g3 (and/c (g2 (view-proxy (list (list "join" #t) (list "fetch" #t)) #t) #f)))
      any)))

  (define/contract (f x y)
    example/c
    (fetch (join x y)))

  (f (open-view "test.db" "students") (open-view "test.db" "test")))