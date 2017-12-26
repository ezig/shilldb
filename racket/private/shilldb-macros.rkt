#lang racket


#|

suggested form for view/c

(view/c [#:fetch expr ...] #:update)

|#

(require (for-syntax syntax/parse)
         (for-syntax (only-in "api/util.rkt"
                              list-unique?))
         (except-in "shilldb.rkt" view/c))


(begin-for-syntax
  (define valid-modifiers
    (hash "fetch" (list '(#:restrict 0))
          "update" (list '(#:restrict 0))))
  
  (define-syntax-class privilege-name
    #:description "primitive privilege"
    (pattern (~literal +fetch))
    (pattern (~literal +update))
    (pattern (~literal +delete))
    (pattern (~literal +insert))
    (pattern (~literal +where))
    (pattern (~literal +select))
    (pattern (~literal +join)))
  
  (define (privilege-stx->string name)
    (substring (symbol->string (syntax->datum name)) 1))
  
  (define-syntax-class privilege-modifier
    #:description "privilege modifier"
    (pattern (name:keyword val:expr)))
  
  (define (validate-modifiers priv-name ks vs)
    (let ([k-datums (map syntax->datum ks)]
          [valid-mods (map car (hash-ref valid-modifiers priv-name))])
      ;TODO: improve error messages
      (unless (list-unique? k-datums)
        (raise-syntax-error 'view/c "duplicate modifier for privilege"))
      (for-each (λ (k) (unless (member k valid-mods)
                         (raise-syntax-error 'view/c "invalid modifier for privilege"))) k-datums)
      vs))
    
    (define-syntax-class privilege
      #:description "privilege"
      (pattern name:privilege-name
               #:with ((~seq mod-name:keyword mod-val:expr) ...) '()
               #:attr [proxy-args 1] '())
      (pattern [name:privilege-name (~seq mod-name:keyword mod-val:expr) ...]
               #:attr [proxy-args 1]
               ;(println (syntax->list #'(mod-val ...))))))
               (validate-modifiers (privilege-stx->string #'name) (syntax->list #'(mod-name ...)) (syntax->list #'(mod-val ...))))))
  
  (define-syntax (privilege-parse stx)
    (syntax-parse stx
      [(_ p:privilege)
       #`(list #,(privilege-stx->string #`p.name) #t p.proxy-args ...)]))
  
  (define-syntax (view/c stx)
    (syntax-parse stx
      [(_ p:privilege ...)
       #'(view-proxy (list (privilege-parse p) ...) #f)]))
  
  
;(define/contract v
;  (view/c [+fetch #:restrict (λ (v) (where v "grade < 90"))])
;  (open-view "test.db" "students"))
;(fetch v)
           
(view/c [+fetch #:a (λ (v) v)])
  