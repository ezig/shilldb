#lang reader "reader.rkt"

(provide (contract-out [add (-> integer? integer? integer?)]))

(define (add y z) (+ y z))