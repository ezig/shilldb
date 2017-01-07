#lang racket

(require db)

(provide (all-defined-out))
(provide (struct-out column))
(provide (struct-out table))
(provide (struct-out view))
(provide (struct-out atom))
(provide (struct-out exp))
(provide (struct-out cond))
(provide (struct-out clause))
(provide (struct-out ast))

(struct column (cid name type notnull default primary-key) #:transparent)
(struct table (name type-map colnames columns) #:transparent)
(struct view (connection connection-type table colnames where-q updatable insertable deletable) #:transparent)

(struct atom (type is-id? val) #:transparent)
(struct exp (op type e1 e2) #:transparent)
(struct cond (cop e1 e2) #:transparent)
(struct clause (connector c1 c2) #:transparent)
(struct ast (clause-type root) #:transparent)

; Parse utilities
(define/contract (ast-to-string ast)
  (-> ast? string?)
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (if (eq? (ast-clause-type ast) 'where)
           (format "(~a) ~a (~a)" (aux c1) connector (aux c2))
           (format "~a ~a ~a" (aux c1) connector (aux c2)))]
      [(cond cop e1 e2) (format "~a ~a ~a" (aux e1) cop (aux e2))]
      [(exp op type e1 e2) (format "~a ~a ~a" (aux e1) op (aux e2))]
      [(atom type is-id? val)
       (if (and (not is-id?) (eq? type 'str))
           (format "'~a'" val)
           (~a val))]))
  (if (null? (ast-root ast))
      ""
      (aux (ast-root ast))))

; SQL utilities
(define (connection-type-error type)
  (error 'connection-type-error "unsupported connection type ~a" type))

(define (type-to-sym connection-type type)
  (case connection-type
    [(sqlite3)
     (if (or (string=? type "text") (string-contains? type "char"))
         'str
         'num)]
    [else (connection-type-error connection-type)]))

(define/contract (where-clause-string v)
  (-> view? string?)
  (case (view-connection-type v)
    [(sqlite3) (let ([where-q (ast-to-string (view-where-q v))])
                 (if (non-empty-string? where-q)
                     (format "where ~a" (ast-to-string (view-where-q v)))
                     ""))]
    [else (connection-type-error (view-connection-type v))]))

(define/contract (query-string v)
  (-> view? string?)
  (case (view-connection-type v)
    [(sqlite3)
     (let ([colnames (string-join (view-colnames v) ",")]
           [tablename (table-name (view-table v))]
           [where-q (where-clause-string v)])
       (format "select ~a from ~a ~a" colnames tablename where-q))]
    [else (connection-type-error (view-connection-type v))]))

(define/contract (delete-query-string v)
  (-> (and/c view? view-deletable) string?)
  (case (view-connection-type v)
    [(sqlite3)
     (let ([tname (table-name (view-table v))]
           [where-q (where-clause-string v)])
       (format "delete from ~a ~a" tname where-q))]
    [else (connection-type-error (view-connection-type v))]))

(define/contract (update-query-string v set-query)
  (-> (and/c view? (λ (v) (not (null? (view-updatable v))))) string? string?)
  (case (view-connection-type v)
    [(sqlite3)
     (let ([tname (table-name (view-table v))]
           [where-q (where-clause-string v)])
       (format "update ~a set ~a ~a" tname set-query where-q))]
    [else (connection-type-error (view-connection-type v))]))

(define/contract (insert-query-string v cols values)
  (-> (and/c view? view-insertable) (listof string?) list? string?)
  (case (view-connection-type v)
    [(sqlite3)
     (let* ([tname (table-name (view-table v))]
            [cols (string-join cols ",")]
            [values (map (λ (v) (if (sql-null? v)
                                    "null"
                                    (if (string? v)
                                        (format "'~a'" v)
                                        (~a v)))) values)]
            [values (string-join values ",")])
       (format "insert into ~a (~a) values (~a)" tname cols values))]
    [else (connection-type-error (view-connection-type v))]))

; General utilities
(define (zip l1 l2) (map cons l1 l2))

(define (list-unique? l)
  (define (helper l seen)
    (if (empty? l)
        #t
        (let ([hd (car l)]
               [tl (cdr l)])
           (if (member hd seen)
               #f
               (helper tl (cons hd seen))))))
  (if (<= (length l) 1)
      #t
      (helper l (list))))