#lang racket

(provide open-dbview
         print-fetch-res
         dbview (capability-out dbview)
         (operation-out where select join fetch delete update insert where-in))

(require shill/plugin
         "db_api_impl.rkt")

(define-operation (where/p)
  (where (view : where/p) (cond : string?)
         -> (result : (and/c dbview? (inherit-privileges/c view)))))

(define-operation (select/p)
  (select (view : select/p) (cols : string?)
          -> (result : (and/c dbview? (inherit-privileges/c view)))))

(define-operation (join/p)
  (join (left : join/p)
        (right : join/p)
        (jcond : string?)
        ([prefix] : (list/c string? string?))
        -> (result : (and/c dbview? (inherit-privileges/c left right)))))

(define-operation (fetch/p)
  (fetch (view : fetch/p) -> result))

(define-operation (delete/p)
  (delete (view : delete/p) -> result))

(define-operation (update/p)
  (update (view : update/p) (query : string?) ([where] : string?) -> result))

(define-operation (insert/p)
  (insert (view : insert/p) (cols : string?) (values : list?) -> result))

; XXX: what privileges should be required on each argument view?
; what should the privileges on the resulting view be?
(define-operation (where-in/p)
  (where-in (view : where-in/p) (col : string?) (subv : where-in/p) ([neg] : boolean?)
      -> (result : (and/c dbview? (inherit-privileges/c view subv)))))

(capability dbview (impl))

(define (open-dbview filename table)
  (dbview (make-view-impl filename table)))

; XXX: this should be defined somewhere else
(define (print-fetch-res fr)
  (begin
    (displayln (string-join (car fr) ", "))
    (displayln "-----")
    (for ([row (cdr fr)])
        (displayln (string-join (map ~a row) ", ")))))

(define-instance (where (view : dbview) (cond : string?) -> (result : dbview?))
  (dbview (where-impl (dbview-impl view) cond)))

(define-instance (select (view : dbview) (cols : string?) -> (result : dbview?))
  (dbview (select-impl (dbview-impl view) cols)))

(define-instance (join (left : dbview)
                       (right : dbview)
                       (jcond : string?)
                       ([prefix (list "lhs" "rhs")] : (list/c string? string?))
                       -> (result : dbview?))
  (dbview (join-impl (dbview-impl left) (dbview-impl right) jcond prefix)))

(define-instance (fetch (view : dbview) -> result)
  (fetch-impl (dbview-impl view)))

(define-instance (delete (view : dbview) -> result)
  (delete-impl (dbview-impl view)))

(define-instance (update (view : dbview)
                         (query : string?)
                         ([where ""] : string?) -> result)
  (update-impl (dbview-impl view) query where))

(define-instance (insert (view : dbview)
                         (cols : string?)
                         (values : list?) -> result)
  (insert-impl (dbview-impl view) cols values))

(define-instance (where-in (view : dbview)
                     (col : string?)
                     (subv : dbview)
                     ([neg #f] : boolean?) -> (result : dbview?))
  (dbview (in-impl (dbview-impl view) col (dbview-impl subv) neg)))

(module+ test
  (define/contract v1 (dbview/c fetch/p where-in/p) (open-dbview "test.db" "v1"))
  (define/contract v2 (dbview/c fetch/p where-in/p) (open-dbview "test.db" "v2"))
  (fetch (where-in (where-in v1 "l" v2) "l" v2 #t)))
