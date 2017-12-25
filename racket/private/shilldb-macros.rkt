#lang racket


#|

suggested form for view/c

(view/c [#:fetch expr ...] #:update)

|#

(require (for-syntax syntax/parse)
         (except-in "shilldb.rkt" view/c))


(begin-for-syntax
 
 (define-syntax-class privilege-name
   #:description "primitive privilege"
   (pattern #:fetch)
   (pattern #:update))


 (define-syntax-class privilege
   #:description "privilege"
   (pattern name:privilege-name
            #:with (modifier ...) '())
   (pattern [name:privilege-name modifier ...])))


(define-syntax (privilege-parse stx)
  (syntax-parse stx
    [(_ p:privilege)
     #`(list #,(keyword->string (syntax->datum #`p.name)) #t p.modifier ...)]))
  

(define-syntax (view/c stx)
  (syntax-parse stx
    [(_ p:privilege ...)
     #'(view-proxy (list (privilege-parse p) ...) #f)]))

  (view/c [#:fetch #t #t] #:update)


            


  