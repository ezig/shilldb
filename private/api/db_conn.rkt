#lang racket

(require racket/generic
         db
         (only-in "util.rkt"
                  view-where-q
                  ast-to-string))

(provide (all-defined-out))

(define-generics 
  dbconn
  [connect-and-exec dbconn fun]
  [build-update-query dbconn view set-q]
  [build-delete-query dbconn view]
  [build-insert-query dbconn view cols values]
  [build-fetch-query dbconn view]
  [start-trigger-transact dbconn]
  [end-trigger-transact dbconn conn]
  [install-view-trigger dbconn view trig-type conn]
  [remove-trigger dbconn trig-name conn]
  [parse-type dbconn type-string])

(define install? #t)

(define (connect-and-exec-with-trigger cinfo v trig-type fun)
  (if install?
      (let* ([connection (start-trigger-transact cinfo)]
             [tname (install-view-trigger cinfo v trig-type connection)]
             [res (with-handlers ([exn:fail:sql? (lambda (e) e)])
                    (fun connection))])
        (begin
          (remove-trigger cinfo tname connection)
          (end-trigger-transact cinfo connection)
          (if (exn:fail:sql? res)
              (error trig-type "failed due to view constraint violation: ~a"
                     (ast-to-string (view-where-q v)))
              res)))
      (connect-and-exec cinfo fun)))
