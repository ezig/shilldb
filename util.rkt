#lang racket

(require db)

(provide (all-defined-out))
(provide (struct-out column))
(provide (struct-out table))
(provide (struct-out join-table))
(provide (struct-out view))
(provide (struct-out atom))
(provide (struct-out exp))
(provide (struct-out cond))
(provide (struct-out clause))
(provide (struct-out ast))

(struct column (cid name type notnull default primary-key))
(struct table (name type-map colnames columns))
(struct join-table (type-map colnames views prefixes))
(struct view (conn-info table colnames where-q updatable insertable deletable))

(struct atom (type is-id? val))
(struct exp (op type e1 e2))
(struct cond (cop e1 e2))
(struct clause (connector c1 c2))
(struct ast (clause-type root))

; View utilties
(define (valid-default v colname)
    (let* ([col (hash-ref (table-columns (view-table v)) colname)]
          [default-value-null? (sql-null? (column-default col))]
          [not-null? (equal? 1 (column-notnull col))])
          (not (and not-null? default-value-null?))))

(define (view-get-type-map v)
  (let ([t (view-table v)])
    (if (table? t)
        (table-type-map t)
        (join-table-type-map t))))

(define (table-replace-type-map t tm)
  (if (table? t)
      (struct-copy table t [type-map tm])
      (struct-copy join-table t [type-map tm])))

(define (view-get-colnames v)
  (let ([t (view-table v)])
    (if (table? t)
        (table-colnames t)
        (join-table-colnames t))))

; Parse utilities
(define (ast-to-string ast [id-prefix ""])
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (if (eq? (ast-clause-type ast) 'where)
           (format "(~a) ~a (~a)" (aux c1) connector (aux c2))
           (format "~a, ~a" (aux c1) (aux c2)))]
      [(cond cop e1 e2) (format "~a ~a ~a" (aux e1) cop (aux e2))]
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

; Printing utilities
(define (print-fetch-res fr)
  (begin
    (displayln (string-join (car fr) ", "))
    (displayln "-----")
    (for ([row (cdr fr)])
        (displayln (string-join (map ~a row) ", ")))))
