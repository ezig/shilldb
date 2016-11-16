#lang racket

(require db)
(require "sql_parse.rkt")

(struct column (cid name type notnull default primary-key) #:transparent)
(struct table (name type-map columns) #:transparent)
(struct view (connection table colnames where-fun where-q updatable insertable deletable) #:transparent)

(define (sqlite3-type-to-sym type)
  (if (or (string=? type "text") (string-contains? type "char"))
      'str
      'num))

(define (baseview filename tablename)
  (let* ([connection (sqlite3-connect #:database filename)]
         ; Fix string-append hack for tableinfo to avoid injection attacks
         [tableinfo (query-rows connection
                                (string-append "PRAGMA table_info(" tablename ")"))]
         [columns (map (λ (col) (apply column (vector->list col))) tableinfo)]
         [type-map (make-hash (map (λ (col) (cons (column-name col)
                                       (sqlite3-type-to-sym (column-type col)))) columns))]
         [t (table tablename type-map columns)]
         [column-names (map (λ (col) (column-name col)) (table-columns t))])
    (view connection t column-names (λ (r) #t) null column-names #t #t)))

(define/contract (select v cols)
  (-> view? string? view?)
  (let ([cols (map string-trim (string-split cols))])
    (struct-copy view v
                 [colnames cols]
                 [updatable (set-intersect (view-colnames v) cols)])))

(define (append-to-where where-q cond)
  (if (null? where-q)
                   cond
                   (if (non-empty-string? cond)
                       (string-append "(" where-q ") and " cond)
                       where-q)))

(define/contract (where v cond)
  (-> view? string? view?)
  (let* ([old-q (view-where-q v)]
        [new-q (append-to-where old-q cond)])
  (struct-copy view v [where-q new-q])))

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

(define (zip l1 l2) (map cons l1 l2))

(define/contract (update v set-query [where-cond ""])
  (->* ((λ (v) (and (view? v) (not (null? (view-updatable v))))) string?)
       (string?)
       any/c)
  (let* ([rows (fetch (struct-copy view v [colnames (list "*")]))]
         [table-colnames (map column-name (table-columns (view-table v)))]
         [row-hts (map make-immutable-hash (map (λ (r) (zip table-colnames (vector->list r))) rows))]
         [type-map (table-type-map (view-table v))]
         [rows-to-update (filter (parse-where where-cond type-map) row-hts)]
         [new-rows (apply-update set-query rows-to-update (view-updatable v) type-map)])
    (if (andmap (parse-where (view-where-q v) type-map) new-rows)
        (let* ([update-q (string-append "update " (table-name (view-table v)))]
               [set-q (string-append " set " set-query)]
               [where-q (build-where-clause (append-to-where (view-where-q v) where-cond))]
               [q (string-append update-q set-q where-q)])
          (query-exec (view-connection v) q))
        (raise "update violated view constraints"))))

(define v (baseview "test.db" "students"))
