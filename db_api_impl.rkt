#lang racket

(provide (all-defined-out))

(require db
         "sql_parse.rkt"
         "util.rkt"
         "sqlite3_strings.rkt")

(define (make-view-impl filename tablename)
  (let* ([cinfo (conn-info filename 'sqlite3)]
         ; Fix string-append hack for tableinfo to avoid injection attacks
         [table-query (format "PRAGMA table_info(~a)" tablename)]
         [tableinfo (connect-and-exec cinfo (λ (c) (query-rows c table-query)))]
         [columns (begin (for ([col tableinfo]
                               #:when (and (eq? 'num (type-to-sym 'sqlite3 (vector-ref col 2)))
                                           (not (sql-null? (vector-ref col 4)))))
                               (let ([new-default (string->number (vector-ref col 4))])
                                 (vector-set! col 4 new-default)))
                         (map (λ (col) (apply column (vector->list col))) tableinfo))]
         [column-hash (make-hash (map (λ (col) (cons (column-name col) col)) columns))]
         [type-map (make-hash (map (λ (col) (cons (column-name col)
                                       (type-to-sym 'sqlite3 (column-type col)))) columns))]
         [column-names (map (λ (col) (column-name col)) columns)]
         [t (table tablename type-map column-names column-hash)])
    (view cinfo t column-names empty-where column-names #t #t)))


; check if columns are within the views as a contract
(define/contract (select-impl v cols)
  (-> view? string? view?)
  (let* ([tm (validate-select cols (view-get-type-map v))]
         [cols (map string-trim (string-split cols ","))]
         [insertable (if (view-insertable v)
                         (let* ([cols-unique? (list-unique? cols)]
                                [cols-simple? (subset? cols (view-colnames v))]
                                [valid-defaults? (andmap (λ (c) (valid-default v c))
                                                         (set-subtract (view-colnames v) cols))])
                           (and cols-unique? cols-simple? valid-defaults?))
                         #f)])
    (struct-copy view v
                 [colnames cols]
                 [table (table-replace-type-map (view-table v) tm)]
                 [updatable (set-intersect (view-colnames v) cols)]
                 [insertable insertable])))

(define/contract (where-impl v cond)
  (-> view? string? view?)
  (let* ([new-q (restrict-where (view-where-q v) cond (view-get-type-map v))])
    (struct-copy view v [where-q new-q])))

(define/contract (join-impl v1 v2 jcond [prefix (list "lhs" "rhs")])
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

(define/contract (fetch-impl v)
  (-> view? any)
  (cons (view-colnames v)
    (map vector->list (connect-and-exec (view-conn-info v)
                                        (λ (c) (query-rows c (query-string v)))))))

(define/contract (delete-impl v)
  (-> (and/c view? view-deletable) any/c)
  (connect-and-exec (view-conn-info v)
                    (λ (c) (query-exec c (delete-query-string v)))))

(define/contract (update-impl v set-query [where-cond ""])
  (->* ((and/c view? (λ (v) (not (null? (view-updatable v))))) string?)
       (string?)
       any/c)
  (let* ([new-v (if (non-empty-string? where-cond)
                    (where-impl v where-cond)
                    v)]
         [trigger (trigger-for-view v)])
    (begin
      (validate-update set-query (view-updatable v) (table-type-map (view-table v)))
      (exec-update-with-trigger
        (view-conn-info v)
        trigger
        (λ (c) (query-exec c (update-query-string new-v set-query)))))))

; values as values not as strings
; data/collections collections lib
(define/contract (insert-impl v cols values)
  (-> (and/c view? view-insertable) string? list? any/c)
  (let* ([cols (map string-trim (string-split cols ","))]
         [valid-cols? (subset? cols (view-colnames v))]
         [missing-cols (set-subtract (table-colnames (view-table v)) cols)]
         [valid-defaults? (andmap (λ (c) (valid-default v c)) missing-cols)])
    (if (not valid-cols?)
        (raise "insert invalid column names for view")
        (if (not valid-defaults?)
            (raise "insert invalid defaults for missing columns")
            (let ([trigger (trigger-for-view v)])
              (exec-insert-with-trigger
                (view-conn-info v)
                trigger
                (λ (c) (query-exec c (insert-query-string v cols values)))))))))
