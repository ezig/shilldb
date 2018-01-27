#lang racket

(provide get-language-info)
 
(define (get-language-info data)
  (lambda (key default)
    (case key
      [else default])))