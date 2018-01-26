#lang racket

(provide enhance-blame/c
         list-assoc
         mutator-redirect-proc)

(struct enhance-blame/c (ctc msg)
  #:property prop:contract
  (build-contract-property
   #:projection
   (λ (ctc)
     (define inner-ctc (enhance-blame/c-ctc ctc))
     (define msg (enhance-blame/c-msg ctc))
     (λ (blame)
       (define new-blame 
         (blame-add-context 
          blame 
          (string-append msg " (insufficient privileges for " msg "!) in")))
       (λ (val)
         (((contract-projection inner-ctc) new-blame) val))))))

(define (mutator-redirect-proc i v) v)

(define (list-assoc key full-details)
    (define in-list? (assoc key full-details))
    (if in-list? in-list? (list key #f)))
