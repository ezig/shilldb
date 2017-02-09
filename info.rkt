#lang info

(define shill-plugin-name 'db)
(define shill-plugin-require '("db_api.rkt"))
(define shill-plugin-ambient '(make-dbview))
(define shill-plugin-interfaces '(dbview))
