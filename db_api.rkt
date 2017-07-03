#lang racket

(provide open-dbview
         dbview (capability-out dbview)
         (operation-out where select join fetch delete update insert))

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

(capability dbview (impl))

(define (open-dbview filename table)
  (dbview (make-view-impl filename table)))

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

(module+ test
  (define v (open-dbview "test.db" "test"))
  (define/contract vc
    (dbview/c update/p where/p fetch/p) v)
  (update (where vc "a < 6") "a = a + 4"))
  ;(update (where vc "a < 7") "a = a + 5"))
  ; (define v1 (make-dbview "test.db" "v1"))
  ; (define j (join v v1 "lhs_b = rhs_l"))
; (fetch (where (select j "lhs_a") "lhs_a <= 3")))

