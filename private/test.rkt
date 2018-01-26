#lang racket

(require "shilldb-macros.rkt")

(->i/join ([X (u) #:post (λ (v) (where v u))])
          ([u string?])
          [(view/c +join +fetch +where) #:groups X]
          [(view/c +join +fetch +where) #:groups X]
          any)