#lang racket

(require "db_api.rkt")

(define (fetch-pre/c pre)
  (make-contract
   #:projection
   (λ (b)
     (λ (v) (fetch (pre v))))))

(define (where-pre/c pre)
  (make-contract
   #:projection
   (λ (b)
     (λ (v)
       (λ (w)
         (where (pre v) w))))))

(struct view-wrap ([fetch #:mutable] [where #:mutable]))

; If pre transformers aren't provided, use the identity function
(define (view-wrap/c #:fetch-pre [fetch-pre (λ (v) v)] #:where-pre [where-pre (λ (v) v)])
  (struct/dc view-wrap
             [fetch (fetch-pre/c fetch-pre)]
             [where (where-pre/c where-pre)]))

(define (make-view-wrap v)
  (view-wrap v v))

(define/contract wv
  (view-wrap/c #:fetch-pre (λ (v) (select v "a")) #:where-pre (λ (v) (select v "b")) )
  (local
    [(define/contract v (dbview/c fetch/p where/p select/p) (open-dbview "test.db" "test"))]
    (make-view-wrap v)))

(module+ test
  ; Demonstrate that the where and fetch accessor see different views
  ; (fetch accessor has "b" column projected away while where accesor
  ; has "a" column projected away, so the where clause will be rejected)
  (view-wrap-fetch wv)
  ((view-wrap-where wv) "a < 10"))