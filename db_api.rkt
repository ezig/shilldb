#lang racket

(require db)
(require "sql_parse.rkt")
(require "util.rkt")

(define (create-view filename tablename)
  (let* ([cinfo (conn-info filename 'sqlite3)]
         ; Fix string-append hack for tableinfo to avoid injection attacks
         [tableinfo (connect-and-exec
                     cinfo
                     (λ (c) (query-rows c (string-append "PRAGMA table_info(" tablename ")"))))]
         [columns (map (λ (col) (apply column (vector->list col))) tableinfo)]
         [column-hash (make-hash (map (λ (col) (cons (column-name col) col)) columns))]
         [type-map (make-hash (map (λ (col) (cons (column-name col)
                                       (type-to-sym 'sqlite3 (column-type col)))) columns))]
         [column-names (map (λ (col) (column-name col)) columns)]
         [t (table tablename type-map column-names column-hash)])
    (view cinfo t column-names empty-where column-names #t #t)))

(define (valid-default v colname)
    (let* ([col (hash-ref (table-columns (view-table v)) colname)]
          [default-value-null? (sql-null? (column-default col))]
          [not-null? (equal? 1 (column-notnull col))])
          (not (and not-null? default-value-null?))))

; check if columns are within the views as a contract 
(define/contract (select v cols)
  (-> view? string? view?)
  (let* ([cols (map string-trim (string-split cols ","))]
         [insertable (if (view-insertable v)
                         (let* ([cols-unique? (list-unique? cols)]
                                [cols-simple? (subset? cols (view-colnames v))]
                                [valid-defaults? (andmap (λ (c) (valid-default v c))
                                                         (set-subtract (view-colnames v) cols))])
                           (and cols-unique? cols-simple? valid-defaults?))
                         #f)])
    (struct-copy view v
                 [colnames cols]
                 [updatable (set-intersect (view-colnames v) cols)]
                 [insertable insertable])))

(define/contract (where v cond)
  (-> view? string? view?)
  (let* ([new-q (restrict-where (view-where-q v) cond (view-get-type-map v))])
    (struct-copy view v [where-q new-q])))

(define/contract (join v1 v2 jcond [prefix (list "lhs" "rhs")])
  (->* (view? view? string?)
       ((and/c list (λ (p) (eq? 2 (length p))) (λ (p) (not (eq? (car p) (cdr p))))))
       view?)
  (let* ([cinfo (view-conn-info v1)]
         [tm1 (view-get-type-map v1)]
         [tm2 (view-get-type-map v2)]
         [type-map (make-hash)])
    (begin
      (hash-for-each tm1 (λ (k v)
                           (hash-set! type-map (format "~a_~a" (first prefix) k) v)))
      (hash-for-each tm2 (λ (k v)
                           (hash-set! type-map (format "~a_~a" (second prefix) k) v)))
      (let* ([join-q (parse-where jcond type-map)]
             [v1-colnames (map (λ (c) (format "~a_~a" (first prefix) c)) (view-get-colnames v1))]
             [v2-colnames (map (λ (c) (format "~a_~a" (second prefix) c)) (view-get-colnames v2))]
             [colnames (append v1-colnames v2-colnames)]
             [jtable (join-table type-map colnames (list v1 v2) prefix)])
        (view cinfo jtable colnames join-q null #f #f)))))

(define/contract (fetch v)
  (-> view? any)
  (connect-and-exec (view-conn-info v)
                    (λ (c) (query-rows c (query-string v)))))

(define/contract (delete v)
  (-> (and/c view? view-deletable) any/c)
  (connect-and-exec (view-conn-info v)
                    (λ (c) (query-exec c (delete-query-string v)))))

(define/contract (update v set-query [where-cond ""])
  (->* ((and/c view? (λ (v) (not (null? (view-updatable v))))) string?)
       (string?)
       any/c)
  (let* ([new-v (if (non-empty-string? where-cond)
                       (where v where-cond)
                       v)]
         [rows (fetch (struct-copy view new-v [colnames (list "*")]))]
         [colnames (table-colnames (view-table v))]
         [row-hts (map make-immutable-hash (map (λ (r) (zip colnames (vector->list r))) rows))]
         [type-map (table-type-map (view-table v))]
         [new-rows (apply-update set-query row-hts (view-updatable v) type-map)])
    (if (andmap (where-to-fun (view-where-q v)) new-rows)
        (connect-and-exec (view-conn-info v)
                          (λ (c) (query-exec c (update-query-string new-v set-query))))
        (raise "update violated view constraints"))))

; values as values not as strings
; data/collections collections lib
(define/contract (insert v cols values)
  (-> (and/c view? view-insertable) string? list? any/c)
  (let* ([cols (map string-trim (string-split cols ","))]
         [valid-cols? (subset? cols (view-colnames v))]
         [missing-cols (set-subtract (table-colnames (view-table v)) cols)]
         [valid-defaults? (andmap (λ (c) (valid-default v c)) missing-cols)])
    (if (not valid-cols?)
        (raise "insert invalid column names for view")
        (if (not valid-defaults?)
            (raise "insert invalid defaults for missing columns")
            (let* ([defaults (map (λ (c)
                                    (column-default (hash-ref (table-columns (view-table v)) c))) missing-cols)]
                   [all-cols (append cols missing-cols)]
                   [all-values (append values defaults)]
                   [new-row (make-immutable-hash (zip all-cols all-values))]
                   [type-map (table-type-map (view-table v))]
                   [valid-row? ((where-to-fun (view-where-q v)) new-row)])
                   (if (not valid-row?)
                       (raise "insert violated view constraints")
                       (connect-and-exec
                        (view-conn-info v)
                        (λ (c) (query-exec c (insert-query-string v all-cols all-values))))))))))
                   
(define v1 (create-view "test.db" "v1"))
(define v2 (create-view "test.db" "v2"))
(define v3 (create-view "test.db" "v3"))
(define v4 (create-view "test.db" "v4"))
