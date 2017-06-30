#lang racket

(provide (all-defined-out))
(provide (struct-out trigger))

(struct trigger (name tname when-clause) #:transparent)

(define/contract (sqlite3-create-update-trigger t)
  (-> trigger? string?)
  (format
   (string-join
    '("CREATE TRIGGER ~a"
        "BEFORE UPDATE"
        "ON ~a"
      "BEGIN"
        "SELECT"
        "CASE"
          "WHEN NOT (~a) THEN"
          "RAISE (ABORT, 'Update violated view constraints')"
        "END;"
      "END;")) (trigger-name t) (trigger-tname t) (trigger-when-clause t)))

(define/contract (sqlite3-remove-trigger t)
  (-> trigger? string?)
  (format "DROP TRIGGER ~a" (trigger-name t)))
