#lang racket

(provide (all-defined-out))

(require db
         "sql_parse.rkt"
         "util.rkt"
         "sqlite3_db_conn.rkt"
         "db_conn.rkt")

(define (get-fk-infos tablename cinfo)
  (let* ([fk-query (format "PRAGMA foreign_key_list(~a)" tablename)]
         [fk-rows (with-handlers ([exn:fail? (λ (e) null)])
                    (connect-and-exec cinfo (λ (c) (query-rows c fk-query))))])
    (map (λ (r)
           (let ([table (vector-ref r 2)]
                 [fk-col (vector-ref r 3)]
                 [ref-col (vector-ref r 4)])
             (fk-constraint table ref-col fk-col))) fk-rows)))

(define (make-view-impl filename tablename)
  (let* ([cinfo (sqlite3-db-conn filename)]
         ; Fix string-append hack for tableinfo to avoid injection attacks
         [table-query (format "PRAGMA table_info(~a)" tablename)]
         [tableinfo (connect-and-exec cinfo (λ (c) (query-rows c table-query)))]
         [columns (begin (for ([col tableinfo]
                               #:when (and (eq? 'num (parse-type cinfo (vector-ref col 2)))
                                           (not (sql-null? (vector-ref col 4)))))
                               (let ([new-default (string->number (vector-ref col 4))])
                                 (vector-set! col 4 new-default)))
                         (map (λ (col) (apply column (vector->list col))) tableinfo))]
         [column-hash (make-hash (map (λ (col) (cons (column-name col) col)) columns))]
         [type-map (make-hash (map (λ (col) (cons (column-name col)
                                       (parse-type cinfo (column-type col)))) columns))]
         [column-names (map (λ (col) (column-name col)) columns)]
         [fks (get-fk-infos tablename cinfo)]
         [t (table tablename type-map column-names column-hash fks)])
    (view cinfo t column-names empty-where '() column-names #t #t)))

; check if columns are within the views as a contract
(define/contract (select-impl v cols)
  (-> view? string? view?)
  (let* ([tm (validate-select cols (view-get-type-map v))])
    (select-aggr-common v cols tm)))

(define/contract (aggregate-impl v cols #:groupby [groupby #f] #:having [having #f])
  (->* (view? string?)
      (#:groupby (or/c string? (curry eq? #f))
       #:having (or/c string? (curry eq? #f)))
      view?)
  (let* ([old-tm (view-get-type-map v)]
         [tm (validate-aggr cols old-tm)])
    ; If both groupby and having are missing, fall back to select logic
    (if (nor groupby having)
        (select-aggr-common v cols tm)
        ; XXX should this be old type map or new type map?
        (let* ([groupby-ast (if groupby
                                (parse-groupby groupby old-tm)
                                #f)]
               [having-ast (if having
                               (parse-having having old-tm)
                               #f)]
               [colnames (map string-trim (string-split cols ","))]
               [cinfo (view-conn-info v)]
               [t (aggr-table tm colnames v groupby-ast having-ast)])
          (view cinfo t colnames empty-where '() null #f #f)))))

(define (select-aggr-common v cols tm)
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
                 [table (table-replace-type-map (view-table v) tm)]
                 [updatable (set-intersect (view-colnames v) cols)]
                 [insertable insertable])))
  
(define/contract (where-impl v where-clause)
  (-> view? string? view?)
  (let* ([new-q (restrict-where (view-where-q v) where-clause (view-get-type-map v))])
    (struct-copy view v [where-q new-q])))

(define (join-impl v1 v2 jcond [prefix (list "lhs" "rhs")])
  (let* ([cinfo (view-conn-info v1)]
         [tm1 (view-get-type-map v1)]
         [tm2 (view-get-type-map v2)]
         [type-map (make-hash)])
    (begin
      (hash-for-each tm1 (λ (k v)
                           (hash-set! type-map (format "~a" k) v))) ;(format "~a_~a" (first prefix) k) v)))
      (hash-for-each tm2 (λ (k v)
                           (hash-set! type-map (format "~a" k) v))) ;(format "~a_~a" (second prefix) k) v)))
      (let* ([join-q (parse-where jcond type-map)]
             [v1-colnames (view-get-colnames v1)] ;(map (λ (c) (format "~a_~a" (first prefix) c)) (view-get-colnames v1))]
             [v2-colnames (view-get-colnames v2)] ;(map (λ (c) (format "~a_~a" (second prefix) c)) (view-get-colnames v2))]
             [colnames (append v1-colnames v2-colnames)]
             [jtable (join-table type-map colnames (list v1 v2) prefix)])
        (view cinfo jtable colnames join-q '() null #f #f)))))

(define/contract (fetch-impl v)
  (-> view? any)
  (let* ([conn (view-conn-info v)]
         [fetch-q (build-fetch-query conn v)])
    (cons (view-colnames v)
          (map vector->list (connect-and-exec conn
                                              (λ (c) (query-rows c fetch-q)))))))

(define/contract (delete-impl v)
  (-> (and/c view? view-deletable) any/c)
  (let* ([conn (view-conn-info v)]
         [delete-q (build-delete-query conn v)])
    (connect-and-exec conn
                    (λ (c) (query-exec c delete-q)))))

(define/contract (update-impl v set-query [where-cond ""])
  (->* ((and/c view? (λ (v) (not (null? (view-updatable v))))) string?)
       (string?)
       any/c)
  (let* ([new-v (if (non-empty-string? where-cond)
                    (where-impl v where-cond)
                    v)])
    (begin
      (validate-update set-query (view-updatable v) (table-type-map (view-table v)))
      (let* ([cinfo (view-conn-info v)]
             [update-q (build-update-query (view-conn-info v) new-v set-query)])
        (connect-and-exec-with-trigger cinfo v 'update
                                       (λ (c) (query-exec c update-q)))))))

(define/contract
  (in-impl v col subv [neg #f])
  (->i ([v view?]
        [col (v) (and/c string?
                        (lambda (col) (member col (view-updatable v))))]
        [subv (and/c view?
                     (lambda (v) (eq? 1 (length (view-colnames v)))))]
        [neg boolean?])
       [result view?])
  (struct-copy view v [ins (append (view-ins v) (list (in-cond col subv neg)))]))

; values as values not as strings
; data/collections collections lib
; add more of the error handlig into the contract
(define/contract (insert-impl v cols values)
  (->i ([v (and/c view? view-insertable)]
        [cols string?]
        [values (cols) (and/c list?
            (lambda (vs) (equal? (length vs) (length (string-split cols ",")))))])
        [result any/c])
  (let* ([cols (map string-trim (string-split cols ","))]
         [valid-cols? (subset? cols (view-colnames v))]
         [missing-cols (set-subtract (table-colnames (view-table v)) cols)]
         [valid-defaults? (andmap (λ (c) (valid-default v c)) missing-cols)])
    (if (not valid-cols?)
        (error "insert invalid column names for view")
        (if (not valid-defaults?)
            (error "insert invalid defaults for missing columns")
            (let* ([cinfo (view-conn-info v)]
                   [insert-q (build-insert-query cinfo v cols values)])
              (connect-and-exec-with-trigger cinfo v 'insert
                                             (λ (c) (query-exec c insert-q))))))))
