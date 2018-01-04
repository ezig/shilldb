#lang racket

(require db)

(provide (all-defined-out))
(provide (struct-out column))
(provide (struct-out table))
(provide (struct-out join-table))
(provide (struct-out view))
(provide (struct-out atom))
(provide (struct-out exp))
(provide (struct-out condexp))
(provide (struct-out clause))
(provide (struct-out ast))

(struct column (cid name type notnull default primary-key))
(define-struct fk-constraint (table ref-col fk-col))
(struct table (name type-map colnames columns fks))
(struct join-table (type-map colnames views prefixes))
(struct aggr-table (type-map colnames view groupby having))
(struct in-cond (column subv neg))
(struct view (conn-info table colnames where-q ins updatable insertable deletable))

(struct atom (type is-id? val))
(struct exp (op type e1 e2))
(struct condexp (cop e1 e2))
(struct clause (connector c1 c2))
(struct ast (clause-type root))

; View utilties
(define (view-get-fks v)
  (match (view-table v)
    [(table _ _ _ _ fks) fks]
    [(join-table _ _ vs _) (append (map view-get-fks vs))]
    [(aggr-table _ _ v _ _) (view-get-fks v)]))

; Assumes that all columns have unique names
(define (view-get-tname-for-col v col)
  (match (view-table v)
    [(table name _ colnames _ _)
     (if (member col colnames) name #f)]
    [(join-table _ _ views _)
     (first (filter values (map view-get-tname-for-col view)))]
    [(aggr-table _ _ v _ _) (view-get-tname-for-col v)]))

(define (valid-default v colname)
    (let* ([col (hash-ref (table-columns (view-table v)) colname)]
          [default-value-null? (sql-null? (column-default col))]
          [not-null? (equal? 1 (column-notnull col))])
          (not (and not-null? default-value-null?))))

(define (view-get-type-map v)
  (match (view-table v)
    [(table _ tm _ _ _) tm]
    [(join-table tm _ _ _) tm]
    [(aggr-table tm _ _ _ _) tm]))
    
(define (table-replace-type-map t tm)
  (let ([t (view-table t)])
    (match t
      [(table _ _ _ _ _) (struct-copy table t [type-map tm])]
      [(join-table _ _ _ _) (struct-copy join-table t [type-map tm])]
      [(aggr-table _ _ _ _ _) (struct-copy aggr-table t [type-map tm])])))
  
(define (view-get-colnames v)
   (match (view-table v)
    [(table _ _ cns _ _) cns]
    [(join-table _ cns _ _) cns]
    [(aggr-table _ cns _ _ _) cns]))

; Parse utilities
(define (ast-to-string ast [id-prefix ""])
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (if (eq? (ast-clause-type ast) 'where)
           (format "(~a) ~a (~a)" (aux c1) connector (aux c2))
           (format "~a, ~a" (aux c1) (aux c2)))]
      [(condexp cop e1 e2) (format "~a ~a ~a" (aux e1) cop (aux e2))]
      [(exp op type e1 e2) (format "~a ~a ~a" (aux e1) op (aux e2))]
      [(atom type is-id? val)
       (if (and (not is-id?) (eq? type 'str))
           (format "'~a'" val)
           (if is-id?
               (format "~a~a" id-prefix val)
               (~a val)))]))
  (if (null? (ast-root ast))
      ""
      (aux (ast-root ast))))

; General utilities
(define (hash-union h1 h2)
  (let* ([h3 (make-hash)]
         [insert-h3 (λ (k v) (hash-set! h3 k v))])
    (begin
      (hash-for-each h1 insert-h3)
      (hash-for-each h2 insert-h3)
      h3)))

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

(define (string-merge s1 s2 joiner)
  (if (non-empty-string? s1)
      (if (non-empty-string? s2)
          (string-join (list s1 s2) joiner)
          s1)
      s2))

; Printing utilities
(define (print-fetch-res fr)
  (begin
    (displayln (string-join (car fr) ", "))
    (displayln "-----")
    (for ([row (cdr fr)])
        (displayln (string-join (map ~a row) ", ")))))
