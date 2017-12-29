#lang racket

(require (for-syntax syntax/parse
                     racket/syntax
                     (only-in racket
                              remove-duplicates
                              cons?
                              empty?
                              flatten)))
(begin-for-syntax
  (define-syntax-class jarg
    #:description "join arg"
    (pattern [ctc:expr #:groups groups:id ...])
    (pattern ctc:expr
             #:with (groups:id ...) '()))

  (define (flatten-once lst)
    (apply append (map (lambda (e) (if (cons? e) e (list e))) lst)))

  ; XXX: Check for syntax errors
  (define (handle-jargs ids jargs ctcs)
    (define (make-constraint-ids)
      (map (λ (groups) (if (empty? groups)
                           '()
                           (list (generate-temporary) (generate-temporary))))
            jargs))
    (define (make-constraint-hash constraint-ids)
      (let ([jhash (make-hash)])
        (begin
          ; Make hash entry for each declared group id
          (for-each (λ (i) (hash-set! jhash (syntax->datum i) null)) ids)
          (map (λ (groups constraints)
                 (for-each (λ (group)
                             (let* ([g-datum (syntax->datum group)]
                                    [old-hash-v (hash-ref jhash g-datum)]
                                    [new-hash-v (cons (cadr constraints) old-hash-v)])                             
                               (hash-set! jhash g-datum new-hash-v)))                                      
                           groups))
               jargs
               constraint-ids)
          jhash)))
      
    (let* ([constraint-ids (make-constraint-ids)]
           [jhash (make-constraint-hash constraint-ids)])
      #`(let-values #,(map (λ (p) #`[#,(map (λ (i) (format-id #f "~a" i)) p) (make-new-group)]) (filter (compose not empty?) constraint-ids))
          #,(flatten-once
             (syntax->datum
              #`(-> #,(map (λ (j ctc own)
                             (if (empty? j)
                                 #`#,ctc
                                 (remove-duplicates (flatten (syntax->datum #`(and/c #,ctc
                                                                                     #,(car own)
                                                                                     #,(map (λ (g) (hash-ref jhash (syntax->datum g)))
                                                                                            j)))))))
                             jargs
                             ctcs
                             constraint-ids))))))))

(define-syntax (->/j stx)
  (syntax-parse stx
    [(_ ([X:id] ...) jargs:jarg ...)
     (handle-jargs (syntax->list #'(X ...)) (map syntax->list (syntax->list #`((jargs.groups ...) ...)))
                   (syntax->list #'(jargs.ctc ...)))]))

(struct dummy
   ([constraint #:mutable #:auto])
  #:auto-value values)

(define (make-new-group)

  (define store (box '()))


   (struct valid-arg/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (if (member val (unbox store))
               val
               (raise-blame-error
                blame
                val
                "not one of the expected-arguments")))))))

  (struct record-arg/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (set-box! store (cons val (unbox store)))
           val)))))
  
  (struct constraint-args/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (define redirect-proc (λ (s v) v))
       (define new-constraint/c (-> (valid-arg/c) (valid-arg/c) any))
       (λ (blame)
         (define new-constraint ((contract-projection new-constraint/c) blame))
         (λ (val)
           (set-box! store (cons val (unbox store)))
           (impersonate-struct val
                               dummy-constraint (λ (s v) (compose new-constraint v))
                               set-dummy-constraint! redirect-proc))))))
  (values (constraint-args/c) (record-arg/c)))

(define (binop d1 d2)
  (define (real-binop d1 d2) d1)
  (((compose (dummy-constraint d1)
             (dummy-constraint d2))
    real-binop)
  d1
  d2))

(define example/c
  (->/j ([X]
         [Y])
        [dummy? #:groups X Y]
        [dummy? #:groups X]
        [dummy? #:groups Y]
        dummy?))

(define/contract (f x y z)
  example/c
  (binop x y))

(f (dummy) (dummy) (dummy))
  