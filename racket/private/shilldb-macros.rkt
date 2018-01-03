#lang racket




(provide view/c
         ->j)

(require (for-syntax syntax/parse
                     (only-in racket/syntax
                              format-id
                              generate-temporary)
                     (only-in racket
                              remove-duplicates
                              cons?
                              empty?
                              curry
                              flatten))
         (except-in "shilldb.rkt" view/c))

#|

suggested form for view/c

(view/c [#:fetch expr ...] #:update)

|#


(begin-for-syntax
  (define-syntax-class jarg
    #:description "join arg"
    (pattern [ctc:expr #:groups groups:id ...])
    (pattern ctc:expr
             #:with (groups:id ...) '()))

  (define (flatten-once lst)
    (apply append (map (lambda (e) (if (cons? e) e (list e))) lst)))

  ; XXX: Check for syntax errors
  (define (handle-jargs ids constraints dctcs jargs ctcs)
    (define (make-constraint-ids)
      (map (λ (groups) (if (empty? groups)
                           '()
                           (list (generate-temporary) (generate-temporary))))
           jargs))
    (define (make-constraint-hash constraint-ids)
      (let ([jhash (make-hash)]
            [chash (make-hash)])
        (begin
          ; Make hash entry for each declared group id
          (for-each (λ (i) (hash-set! jhash (syntax->datum i) null)) ids)
          (for-each (λ (i c d) (hash-set! chash (syntax->datum i) (cons c d))) ids constraints dctcs)
          (map (λ (groups constraints)
                 (for-each (λ (group)
                             (let* ([g-datum (syntax->datum group)]
                                    [old-hash-v (hash-ref jhash g-datum)]
                                    [new-hash-v (cons (cadr constraints) old-hash-v)])                             
                               (hash-set! jhash g-datum new-hash-v)))                                      
                           groups))
               jargs
               constraint-ids)
          (values jhash chash))))
    (define (make-arg-contract jhash chash jarg ctc constraint-ids)
      (if (empty? jarg)
          #`#,ctc
          #`(and/c #,ctc
                   #,(car constraint-ids)
                   #,((compose remove-duplicates flatten-once flatten-once syntax->datum)
                      #`(and/c #,(map (λ (group)
                                        (let ([d (syntax->datum group)])
                                          (map (λ (c)
                                                 (if (member c constraint-ids)
                                                     #`any/c
                                                     #`(#,c
                                                        #,(car (hash-ref chash d))
                                                        #,(cdr (hash-ref chash d)))))
                                               (hash-ref jhash d))))
                                      jarg))))))
  
    (let*-values ([(constraint-ids) (make-constraint-ids)]
                  [(jhash chash) (make-constraint-hash constraint-ids)])
      #`(let-values #,(map (λ (p) #`[#,(map (λ (i) (format-id #f "~a" i)) p) (make-join-group)])
                           (filter (compose not empty?) constraint-ids))
          #,(flatten-once
             (syntax->datum
              #`(-> #,(map (curry make-arg-contract jhash chash) jargs ctcs constraint-ids)))))))
  
  (define valid-modifiers
    (hash "fetch" (list '(#:restrict 0))
          "update" (list '(#:restrict 0))
          "insert" (list '(#:restrict 0))
          "delete" (list '(#:restrict 0))))
  
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
  
  (define (validate-modifiers priv-name ks vs priv-stx)
    (define (check-valid-names)
      (let ([valid-mods (map car (hash-ref valid-modifiers priv-name))])
        (for-each (λ (k) (unless (member (syntax->datum k) valid-mods)
                           (raise-syntax-error 'view/c (format "invalid modifier for privilege ~a" priv-name) priv-stx k)))
                  ks)))
    (define (check-duplicates)      
      (foldl (λ (hd acc)
               (let ([hd-datum (syntax->datum hd)])
                 (if (member hd-datum acc)
                     (raise-syntax-error 'view/c (format "duplicate modifier for privilege ~a" priv-name) priv-stx hd)
                     (cons hd-datum acc))))
             null ks))
    
    (begin
      (check-valid-names)
      (check-duplicates)
      vs))
  
  (define-syntax-class privilege
    #:description "privilege"
    (pattern name:privilege-name
             #:with ((~seq mod-name:keyword mod-val:expr) ...) '()
             #:attr [proxy-args 1] '())
    (pattern [name:privilege-name (~seq mod-name:keyword mod-val:expr) ...]
             #:attr [proxy-args 1]
             (validate-modifiers (privilege-stx->string #'name) (syntax->list #'(mod-name ...)) (syntax->list #'(mod-val ...)) this-syntax))))

(define-syntax (->j stx)
  (syntax-parse stx
    [(_ ([X:id #:post constraint:expr #:with dctc:expr] ...) jargs:jarg ...)
     (handle-jargs (syntax->list #'(X ...))
                   (syntax->list #'(constraint ...))
                   (syntax->list #'(dctc ...))
                   (map syntax->list (syntax->list #`((jargs.groups ...) ...)))
                   (syntax->list #'(jargs.ctc ...)))]))

(define-syntax (privilege-parse stx)
  (syntax-parse stx
    [(_ p:privilege)
     #`(list #,(privilege-stx->string #`p.name) #t p.proxy-args ...)]))
  
(define-syntax (view/c stx)
  (syntax-parse stx
    [(_ p:privilege ...)
     #'(view-proxy (list (privilege-parse p) ...) #f)]))

(module+ test
  (define example/c
  (->j ([X #:post (λ (v) (where v "a = 3")) #:with (view/c +fetch +where)])
       [(view/c +join) #:groups X]
       [(view/c +join) #:groups X]
       any))

  (define/contract (f x y)
    example/c
    (fetch (join x y "")))

  (f (open-view "test.db" "students") (open-view "test.db" "test")))


#|
suggested form for join-constraint/c

(join-constraint/c ([(X ...) constraint] ...) ctc )
|#

(define-syntax (constraint/c stx)
  (syntax-parse stx
    [(_ ([(X:id ...) constraint] ...) ctc)
     #'(let-values ([(X ...) (constraint)] ...)
         ctc)]))




