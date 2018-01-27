#lang racket

(provide shilldb-require
         shilldb-provide)

(require (for-syntax
          syntax/parse
          syntax/modresolve
          racket/require-transform))

(define-syntax-rule (shilldb-require spec ...) (require (check-if-shilldb spec) ...))

(define-syntax check-if-shilldb
  (make-require-transformer
   (lambda (stx)
     (syntax-case stx ()
       [(_ spec)
        (let-values ([(imports sources) (expand-import #'spec)])
          (values
           (begin
             (for/fold ([seen null])
               ([id imports])
               (let* ([src-mod (import-src-mod-path id)]
                      [src-mod-path (resolve-module-path (cond [(module-path? src-mod) src-mod]
                                                               [else (syntax->datum src-mod)])
                                                         #f #;(syntax-source #'spec))]
                      [_ (dynamic-require src-mod-path (void))]
                      [mod-info 
                       (module->language-info src-mod-path)])
                 (cond [(member src-mod-path seen)
                        seen]
                       [(equal? mod-info '#(shilldb/cap/lang/language-info get-language-info #f))
                        (cons src-mod-path seen)]
                       [else (error 
                              'non-shilldb-module-required 
                              "~a not a shilldb/cap module"
                              src-mod-path)]))) 
             imports)
           sources))]))))

(define-syntax shilldb-provide
  (λ (stx)
    (syntax-parse stx
      [(_ spec ...) #'(provide (contract-out spec ...))])))
