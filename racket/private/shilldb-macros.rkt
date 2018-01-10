#lang racket




(provide view/c
         ->j
         ->i/join
         constraint/c)

(require (for-syntax syntax/parse
                     (only-in racket/splicing
                              splicing-let-values)
                     (only-in racket/syntax
                              format-id
                              generate-temporary)
                     (only-in racket
                              remove-duplicates
                              cons?
                              empty?
                              curry
                              flatten
                              last)
                     (only-in "api/util.rkt" zip))
         (except-in "shilldb.rkt" view/c)
         (only-in racket/splicing
         splicing-let-values))

#|

suggested form for view/c

(view/c [#:fetch expr ...] #:update)

|#


(begin-for-syntax
  (define-syntax-class jarg
    #:description "join arg"
    (pattern [((~literal view/c) mod ...) #:groups groups:id ...]
             #:with ctc:expr #'(jview/c mod ...))
    (pattern [ctc:expr #:groups groups:id ...])
    (pattern ctc:expr
             #:with (groups:id ...) '()))

  ; XXX since there are only two modifiers for join groups, enumerating the
  ; the 5 different cases here is easy, but this does not scale up well.
  ; If more modifiers are added, should take a more robust approach like
  ; is used with privilege modifiers
  (define-syntax-class join-group
    #:description "join group"
    #:attributes (name post derive)
    (pattern name:id
             #:with post #f
             #:with derive #f)
    (pattern [name:id #:post post:expr #:with derive:expr])
    (pattern [name:id #:with derive:expr #:post post:expr])
    (pattern [name:id #:post post:expr]
             #:with derive #f)
    (pattern [name:id #:with derive:expr]
             #:with post #f))

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
                                                        #,(let ([derive (cdr (hash-ref chash d))])
                                                            (if (syntax->datum derive)
                                                                derive
                                                                ctc)))))                                                        
                                               (hash-ref jhash d))))
                                      jarg))))))
  
    (let*-values ([(constraint-ids) (make-constraint-ids)]
                  [(jhash chash) (make-constraint-hash constraint-ids)])
      #`(let-values #,(map (λ (p) #`[#,(map (λ (i) (format-id #f "~a" i)) p) (make-join-group)])
                           (filter (compose not empty?) constraint-ids))
          #,(flatten-once
             (syntax->datum
              #`(-> #,(map (curry make-arg-contract jhash chash) jargs ctcs constraint-ids)))))))

  (define-syntax-class dep-join-group
    #:description "dependent join group"
    #:attributes (name [depends 1] post derive)
    (pattern group:join-group
             #:with (depends ...) '()
             #:with name #'group.name
             #:with post #'group.post
             #:with derive #'group.derive)
    (pattern [name:id (depends:id ...) #:post post:expr #:with derive:expr])
    (pattern [name:id (depends:id ...) #:with derive:expr #:post post:expr])
    (pattern [name:id (depends:id ...) #:post post:expr]
             #:with derive #f)
    (pattern [name:id (depends:id ...) #:with derive:expr]
             #:with post #f))

  (define-syntax-class dep-jarg
    #:description "dependent join arg"
    (pattern [name:id ctc:expr]))

  (struct dep-join-group-s (name depends post derive) #:transparent)
  (struct dep-jarg-s (name ctc) #:transparent)

  (define (dep-args-to-struct name ctc)
    (map (curry dep-jarg-s) name ctc))
  
  (define (dep-jgroups-to-struct name depends post derive)
    (map (curry dep-join-group-s)
         name
         (map syntax->list depends)
         post
         derive))
         
  (define (handle-dep-jargs jgroups dep-args jargs)
    (define (make-box-ids)
      (let ([idhash (make-hash)])
        (begin
          (for-each (λ (arg) (hash-set! idhash
                                        (syntax->datum (dep-jarg-s-name arg))
                                        (list (generate-temporary) (generate-temporary))))
                    dep-args)
          idhash)))
    (define idhash (make-box-ids))
    #`(let-values #,(map (λ (arg) #`[(#,(format-id #`jargs "~a" (car (hash-ref idhash (syntax->datum (dep-jarg-s-name arg)))))
                                      #,(format-id #`jargs "~a" (cadr (hash-ref idhash (syntax->datum (dep-jarg-s-name arg))))))
                                     (make-arg-box)]) dep-args)
          (->j #,(map (λ (jgroup) #`[#,(dep-join-group-s-name jgroup)
                                 #:post (λ (v) (let #,(map (λ (dep) #`[#,dep #,(format-id #`jargs "~a"
                                                                                          (car (hash-ref idhash (syntax->datum dep))))])
                                                       (dep-join-group-s-depends jgroup))
                                          (#,(dep-join-group-s-post jgroup) v)))
                                 #:with #,(dep-join-group-s-derive jgroup)]) jgroups)
               #,(flatten-once
                  (list (map syntax->datum
                             (map (λ (arg) #`(and/c #,(dep-jarg-s-ctc arg) #,(format-id #`jargs "~a"
                                                                                        (cadr (hash-ref idhash (syntax->datum (dep-jarg-s-name arg)))))))
                                  dep-args))
                        (flatten-once (syntax->list jargs)))))))
  
  (define valid-modifiers
    (hash "fetch" (list '(#:restrict 0))
          "update" (list '(#:restrict 0))
          "insert" (list '(#:restrict 0))
          "delete" (list '(#:restrict 0))
          "aggregate" (list '(#:having 0) '(#:aggrs 1) '(#:with 2))))
  
  (define-syntax-class privilege-name
    #:description "primitive privilege"
    (pattern (~literal +fetch))
    (pattern (~literal +update))
    (pattern (~literal +delete))
    (pattern (~literal +insert))
    (pattern (~literal +where))
    (pattern (~literal +select))
    (pattern (~literal +aggregate))
    (pattern (~literal +join)))
  
  (define (privilege-stx->string name)
    (substring (symbol->string (syntax->datum name)) 1))
  
  (define-syntax-class privilege-modifier
    #:description "privilege modifier"
    (pattern (name:keyword val:expr)))
  
  (define (validate-modifiers priv-name ks vs priv-stx)
    (define valid-mods
      (begin
        (unless (hash-has-key? valid-modifiers priv-name)
          (raise-syntax-error 'view/c (format "privilege ~a does not support modifiers" priv-name) priv-stx))
        (hash-ref valid-modifiers priv-name)))
    (define (check-valid-names)
      (for-each (λ (k) (unless (assoc (syntax->datum k) valid-mods)
                         (raise-syntax-error 'view/c (format "invalid modifier for privilege ~a" priv-name) priv-stx k)))
                ks))
    (define (check-duplicates)      
      (foldl (λ (hd acc)
               (let ([hd-datum (syntax->datum hd)])
                 (if (member hd-datum acc)
                     (raise-syntax-error 'view/c (format "duplicate modifier for privilege ~a" priv-name) priv-stx hd)
                     (cons hd-datum acc))))
             null ks))
    (define (sort-mods)
      (sort (zip ks vs) <
            #:key (λ (x)
                    (cadr (assoc
                           (syntax->datum (car x))
                           valid-mods)))))
    (define (fill-missing-mods mods)
      (define max-mod (cadr (assoc (syntax->datum (car (last mods))) valid-mods)))
      (for/list  ([idx (in-range (+ 1 max-mod))])
        (let* ([name-for-idx (car (findf (λ (x) (= (cadr x) idx)) valid-mods))]
               [given-mod (assoc name-for-idx (map (λ (m) (cons (syntax->datum (car m)) (cdr m))) mods))])
          (if given-mod
              (cdr given-mod)
              #'#f))))
    (begin
      (check-valid-names)
      (check-duplicates)
      (fill-missing-mods (sort-mods))))
  
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
    [(_ (jgroup:join-group ...) (jargs:jarg ...))
     (datum->syntax #'stx
      (syntax->datum
       (handle-jargs (syntax->list #'(jgroup.name ...))
                     (syntax->list #'(jgroup.post ...))
                     (syntax->list #'(jgroup.derive ...))
                     (map syntax->list (syntax->list #`((jargs.groups ...) ...)))
                     (syntax->list #'(jargs.ctc ...)))))]))

(define-syntax (->i/join stx)
  (syntax-parse stx
    [(_ (jgroup:dep-join-group ...) (dep-args:dep-jarg ...) jargs:jarg ...)
     (let ([jgroup-s (dep-jgroups-to-struct
                      (syntax->list #'(jgroup.name ...))
                      (syntax->list #'((jgroup.depends ...) ...))
                      (syntax->list #'(jgroup.post ...))
                      (syntax->list #'(jgroup.derive ...)))]
           [dep-args-s (dep-args-to-struct
                        (syntax->list #'(dep-args.name ...))
                        (syntax->list #'(dep-args.ctc ...)))])
       (handle-dep-jargs jgroup-s dep-args-s #'(jargs ...)))]))

(define-syntax (privilege-parse stx)
  (syntax-parse stx
    [(_ p:privilege)
     #`(list #,(privilege-stx->string #`p.name) #t p.proxy-args ...)]))
  
(define-syntax (view/c stx)
  (syntax-parse stx
    [(_ p:privilege ...)
     #'(view-proxy (list (privilege-parse p) ...) #f)]))

(define-syntax (jview/c stx)
  (syntax-parse stx
    [(_ p:privilege ...)
     #'(view-proxy (list (privilege-parse p) ...) #t)]))

(module+ test
  (define example/c
  (->j ([X #:post (λ (v) (where v "a = b"))])
       [(view/c +join +fetch +where) #:groups X]
       [(view/c +join +fetch +where) #:groups X]
       any))

  (define/contract x
    (view/c [+aggregate #:aggrs "MIN,max" #:having "max(b) > 10" #:with (view/c +fetch)])
    (open-view "test.db" "test"))

  (fetch (aggregate x "MIN(b)" #:groupby "a" #:having "max(b) < 50")))
      
  ;(f (open-view "test.db" "test") (open-view "test.db" "students")))

(->i/join ([X (u) #:post (λ (v) (where v u))])
          ([u string?])
          [(view/c +join +fetch +where) #:groups X]
          [(view/c +join +fetch +where) #:groups X]
          any)
          

#|
suggested form for join-constraint/c

(join-constraint/c ([(X ...) constraint] ...) ctc )
|#

(define-syntax (constraint/c stx)
  (syntax-parse stx
    [(_ ([(X:id ...) constraint] ...) ctc)
     #'(let-values ([(X ...) (constraint)] ...)
         ctc)]))




