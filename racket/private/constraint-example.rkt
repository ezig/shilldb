#lang racket

(require "shilldb-macros.rkt")

(define (apply-first-and-second-arg-only)

  (define store (box '()))

  (struct record-arg/c ()
    #:property prop:contract
    (build-contract-property
     #:projection
     (λ (ctc)
       (λ (blame)
         (λ (val)
           (set-box! store (cons val (unbox store)))
           val)))))

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
  
  (values
   (record-arg/c)
   (valid-arg/c)))

(define example/c
  (constraint/c ([(X Y) apply-first-and-second-arg-only])
                (-> (and/c X number?)
                    (and/c X number?)
                    number?
                    (-> Y Y number?)
                    number?)))
  


(define/contract (f x y z g)
  example/c
  (g x y))

(f 2 3 4 +)

(define/contract (f-prime x y z g)
  example/c
  (g x z))

;; should fail
;(f-prime 2 3 4 +)

(struct dummy
  ([constraint #:mutable #:auto])
  #:auto-value values)


(define (apply-first-to-second-arg-only-obj)

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
  (constraint-args/c))

(define (binop d1 d2)
  (define (real-binop d1 d2) d1)
  (((compose (dummy-constraint d1)
             (dummy-constraint d2))
    real-binop)
  d1
  d2))
  
(define example-prime/c
  (constraint/c ([(X) apply-first-to-second-arg-only-obj])
                (-> (and/c X dummy?)
                    (and/c X dummy?)
                    dummy?
                    dummy?)))
  
(define/contract (g x y z)
  example-prime/c
  (binop x y))

(g (dummy) (dummy) (dummy))

(define/contract (g-prime x y z)
  example-prime/c
  (binop x z))

;; should fail
;(g-prime (dummy) (dummy) (dummy))


