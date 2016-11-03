#lang racket

(require db)

(struct column (cid name type notnull default primary-key) #:transparent)
(struct table (name columns) #:transparent)
(struct view (connection table colnames where-fun where-q updatable insertable deletable) #:transparent)

(define (baseview filename tablename)
  (let* ([connection (sqlite3-connect #:database filename)]
         ; Fix string-append hack for tableinfo to avoid injection attacks
         [tableinfo (query-rows connection
                                (string-append "PRAGMA table_info(" tablename ")"))]
         [t (table tablename (map (λ (col) (apply column (vector->list col)))
                                  tableinfo))]
         [column-names (map (λ (col) (column-name col)) (table-columns t))])
    (view connection t column-names (λ (r) #t) null #t #t #t)))

(define/contract (select v cols)
  (-> view? (listof string?) view?)
  (struct-copy view v [colnames cols]))

(define comp-to-fun (hash "=" =
                        "<" <
                        "<=" <=
                        ">" >
                        ">=" >=
                        "!=" (λ (v1 v2) (not (= v1 v2)))))

(define (cond-to-fun cond)
  (if (non-empty-string? cond)
      (let* ([conds (map string-trim (string-split cond "and"))]
             [conds (map (λ (c) (map string-trim (string-split c))) conds)]
             [cond-funs (map (λ (c) (λ (r) ((hash-ref comp-to-fun (list-ref c 1)) (hash-ref r (list-ref c 0)) (string->number (list-ref c 2))))) conds)])
        (λ (r) (andmap (λ (c) (c r)) cond-funs)))
      (λ (r) #t)))

(define (append-to-where where-q cond)
  (if (null? where-q)
                   cond
                   (if (non-empty-string? cond)
                       (string-append "(" where-q ") and " cond)
                       where-q)))

(define/contract (where v cond)
  (-> view? string? view?)
  (let* ([old-q (view-where-q v)]
        [old-fun (view-where-fun v)]
        [new-q (append-to-where old-q cond)]
        [cond-fun (cond-to-fun cond)]
        [new-fun (λ (r) (and (cond-fun r) (old-fun r)))])
  (struct-copy view v [where-q new-q] [where-fun new-fun])))

(define (build-where-clause q)
  (if (or (not (non-empty-string? q)) (null? q))
      ""
      (string-append " where " q)))

(define/contract (fetch v)
  (-> view? any)
  (let* ([select-q (string-append "select " (string-join (view-colnames v) ","))]
         [from-q (string-append " from " (table-name (view-table v)))]
         [q (string-append select-q from-q (build-where-clause (view-where-q v)))])
  (query-rows (view-connection v) q)))

(define/contract (delete v)
  (-> (λ (v) (and (view? v) (view-deletable v))) any/c)
  (let ([tname (table-name (view-table v))])
    (query-exec (view-connection v) (string-append "delete from " tname (build-where-clause (view-where-q v))))))

(define (set-query-to-fun q)
  (let* ([qs (map string-trim (string-split q ","))]
         [qs (map (λ (c) (map string-trim (string-split c))) qs)]
         [q-funs (map (λ (q) (λ (r) (hash-set r (list-ref q 0) (string->number (list-ref q 2))))) qs)]
         [q-fun (apply compose (reverse q-funs))])
    q-fun))

(define (zip l1 l2) (map cons l1 l2))

(define/contract (update v set-query [where-cond ""])
  (->* ((λ (v) (and (view? v) (view-updatable v))) string?)
       (string?)
       any/c)
  (let* ([rows (fetch v)]
         [table-colnames (map column-name (table-columns (view-table v)))]
         [row-hts (map make-immutable-hash (map (λ (r) (zip table-colnames (vector->list r))) rows))]
         [update-fun (set-query-to-fun set-query)]
         [rows-to-update (filter (cond-to-fun where-cond) row-hts)]
         [new-rows (map update-fun rows-to-update)])
    (if (andmap (view-where-fun v) new-rows)
        (let* ([update-q (string-append "update " (table-name (view-table v)))]
               [set-q (string-append " set " set-query)]
               [where-q (build-where-clause (append-to-where (view-where-q v) where-cond))]
               [q (string-append update-q set-q where-q)])
          (query-exec (view-connection v) q))
        (raise "update violated view constraints"))))