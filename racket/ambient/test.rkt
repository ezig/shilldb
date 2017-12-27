#lang reader "reader.rkt"

(require "../cap/test.rkt")

(define v (open-view "test.db" "students"))

(add v 2)