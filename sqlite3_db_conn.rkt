#lang racket

(provide (struct-out sqlite3-db-conn))

(require db
         "db_conn.rkt"
         "util.rkt")

(struct sqlite3-db-conn (filename)
  #:methods gen:dbconn
  [(define (connect-and-exec dbconn fun)
     (let ([connection (sqlite3-connect #:database (sqlite3-db-conn-filename dbconn))])
       (begin0
         (fun connection)
         (disconnect connection))))
   (define (build-update-query dbconn view set-q)
     (update-query-string view set-q))
   (define (build-delete-query dbconn view)
     (delete-query-string view))
   (define (build-insert-query dbconn view cols values)
     (insert-query-string view cols values))
   (define (build-fetch-query dbconn view)
     (query-string view))
   (define/contract
     (install-view-trigger dbconn view trig-type [suffix-num 0])
     (->* (dbconn? view? (lambda (t) (or (eq? 'update t) (eq? 'insert t))))
          (number?)
          any/c)
     (let* ([trig-name (format "view_trigger~a" suffix-num)]
            [table-name (table-name (view-table view))]
            [where-q (ast-to-string (view-where-q view) "new.")]
            [when-clause (if (non-empty-string? where-q)
                             where-q
                             "1")]
            [trigger-q (build-trigger-query
                         trig-name
                         table-name
                         when-clause
                         trig-type)])
       ; XXX hacky way to deal with duplicate trigger names
       (with-handlers ([exn:fail? (lambda (e)
                                    (install-view-trigger
                                      dbconn
                                      view
                                      trig-type
                                      (+ suffix-num 1)))])
         (begin
           (connect-and-exec dbconn
                             (lambda (c) (query-exec c trigger-q)))
           trig-name))))
   (define (remove-trigger dbconn trig-name)
     (connect-and-exec dbconn 
                       (lambda (c)
                         (query-exec c (format "DROP TRIGGER ~a" trig-name))
                         )))
   (define (parse-type dbconn type-string)
     (let ([type-lower (string-downcase type-string)])
        (if (or (string=? type-lower "text") (string-contains? type-lower "char"))
            'str
            'num)))
   ])

(define (build-trigger-query trigger-name table-name trigger-when-clause trigger-type)
  (let ([type-string
         (case trigger-type
           [(update) "UPDATE"]
           [(insert) "INSERT"])])
    (format
      (string-join
        '("CREATE TRIGGER ~a"
          "BEFORE ~a"
          "ON ~a"
          "BEGIN"
          "SELECT"
          "CASE"
          "WHEN NOT (~a) THEN"
          "RAISE (ABORT, '~a violated view constraints')"
          "END;"
          "END;")) trigger-name type-string table-name trigger-when-clause type-string)))

(define/contract (where-clause-string v)
  (-> view? string?)
  (let* ([where-q (ast-to-string (view-where-q v))]
         [in-q (string-join (map (lambda (icnd) (format "~a ~a (~a)"
                       (in-cond-column icnd)
                       (if (in-cond-neg icnd) "not in" "in")
                       (query-string (in-cond-subv icnd)))) (view-ins v))
                            " and ")]
         [q (string-merge where-q in-q " and ")])
    (if (non-empty-string? q)
        (format " where ~a" q)
        "")))

(define/contract (query-string v)
  (-> view? string?)
  (define (sqlite3-table-string t)
    (if (table? t)
        (table-name t)
        (let* ([colnames (string-join (join-table-colnames t) ",")]
               [views (join-table-views t)]
               [subqs (for/lists (l1)
                        ([view views]
                         [name (join-table-prefixes t)])
                        (format "(~a)" (sqlite3-query-string view name)))])
               (string-join subqs ","))))
  (define (sqlite3-query-string v [col-prefix null])
    (let* ([colnames (view-colnames v)]
           [colnames (if (null? col-prefix)
                         colnames
                         colnames)]
                         ;(map (λ (c) (format "~a as  ~a_~a" c col-prefix c)) colnames))]
           [colnames (string-join colnames ",")]
           [table-string (sqlite3-table-string (view-table v))]
           [where-q (where-clause-string v)])
      (format "select ~a from (~a)~a" colnames table-string where-q)))
  (sqlite3-query-string v))

(define/contract (delete-query-string v)
  (-> (and/c view? view-deletable) string?)
  (let ([tname (table-name (view-table v))]
        [where-q (where-clause-string v)])
    (format "delete from ~a ~a" tname where-q)))

(define/contract (update-query-string v set-query)
  (-> (and/c view? (λ (v) (not (null? (view-updatable v))))) string? string?)
  (let ([tname (table-name (view-table v))]
        [where-q (where-clause-string v)])
    (format "update ~a set ~a ~a" tname set-query where-q)))

(define/contract (insert-query-string v cols values)
  (-> (and/c view? view-insertable) (listof string?) list? string?)
  (let* ([tname (table-name (view-table v))]
         [cols (string-join cols ",")]
         [values (map (λ (v) (if (sql-null? v)
                                 "null"
                                 (if (string? v)
                                     (format "'~a'" v)
                                     (~a v)))) values)]
         [values (string-join values ",")])
    (format "insert into ~a (~a) values (~a)" tname cols values)))
