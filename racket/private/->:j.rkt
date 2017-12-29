#lang racket

(require (for-syntax syntax/parse
                     racket/syntax
                     (only-in racket
                              remove-duplicates
                              cons?
                              flatten)))
(begin-for-syntax
  (define-syntax-class jarg
    #:description "join arg"
    (pattern [ctc:expr #:groups groups:id ...])
    (pattern ctc:expr
             #:with (groups:id ...) '()))
  
  (define (handle-jargs ids jargs ctcs)
    (let ([jhash (make-hash)])
      (for-each (λ (s) (hash-set! jhash (syntax->datum s) null)) ids)
      (let ([new-ids
             (map (λ (gs)
                    (let ([owner (generate-temporary)]
                          [recorder (generate-temporary)])
                      (for-each (λ (g) (hash-set! jhash (syntax->datum g)
                                                  (cons recorder (hash-ref jhash (syntax->datum g)))))
                                (syntax->list gs))
                      (list owner recorder)))
                  jargs)])
        #`(let-values #,(map (λ (p) #`[#,(map (λ (i) (format-id #f "~a" i)) p) (make-new-group)]) new-ids)
            #,(apply append (map (lambda (e) (if (cons? e) e (list e)))
                                 (syntax->datum
                                  #`(-> #,(map (λ (j ctc own)
                                                 (remove-duplicates (flatten (syntax->datum #`(and/c #,ctc
                                                                   #,(car own)
                                                                   #,(map (λ (g) (hash-ref jhash (syntax->datum g)))
                                                                          (syntax->list j)))))))
                                               jargs
                                               ctcs
                                               new-ids))))))))))

(define-syntax (->/j stx)
  (syntax-parse stx
    [(_ ([X:id] ...) jargs:jarg ...)
     (handle-jargs (syntax->list #'(X ...)) (syntax->list #`((jargs.groups ...) ...))
                   (syntax->list #`(jargs.ctc ...)))]))

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
  