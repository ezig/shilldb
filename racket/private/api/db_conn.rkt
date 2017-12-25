#lang racket

(require racket/generic
         db)

(provide (all-defined-out))

(define-generics 
  dbconn
  [connect-and-exec dbconn fun]
  [build-update-query dbconn view set-q]
  [build-delete-query dbconn view]
  [build-insert-query dbconn view cols values]
  [build-fetch-query dbconn view]
  [install-view-trigger dbconn view trig-type]
  [remove-trigger dbconn trig-name]
  [parse-type dbconn type-string])

(define (connect-and-exec-with-trigger cinfo v trig-type fun)
  (let ([tname (install-view-trigger cinfo v trig-type)]
        [res (with-handlers ([exn:fail:sql? (lambda (e) e)])
               (connect-and-exec cinfo fun))])
    (begin
      (remove-trigger cinfo tname)
      (if (exn:fail:sql? res)
          (error trig-type "failed due to view constraint violation")
          res))))