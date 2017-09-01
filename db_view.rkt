#lang racket

(require racket/generic)

(provide (all-defined-out))
(provide (struct-out conn-info))
(provide (struct-out column))
(provide (struct-out table))
(provide (struct-out join-table))
(provide (struct-out view))

(struct conn-info (filename type))
(struct column (cid name type notnull default primary-key))
(struct table (name type-map colnames columns))
(struct join-table (type-map colnames views prefixes))
(struct view (conn-info table colnames where-q updatable insertable deletable))

(define-generics dbview
  [connect-and-exec dbview fun]
  [get-create-trigger-query dbview]
  [get-query dbview]
  [get-delete-query dbview]
  [get-update-query dbview]
  [get-view dbview]
  [copy-with-view dbview new-view])

