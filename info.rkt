#lang info

(define shill-plugin-name 'db)
(define shill-plugin-require 'db_api/db_api)
(define shill-plugin-ambient '(open-dbview print-fetch-res))
(define shill-plugin-capabilities '(dbview))
(define shill-plugin-operations '(where select join fetch delete update insert))
