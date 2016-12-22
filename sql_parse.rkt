#lang racket

(provide parse-where)
;(provide apply-update)

(require parser-tools/yacc
         parser-tools/lex
         (prefix-in : parser-tools/lex-sre))

(define-tokens value-tokens (IDENTIFIER NUM STR COMP ADDOP MULOP))
(define-empty-tokens op-tokens
  (OP CP            ; ( )
      AND              ; and
      OR               ; or
      COMMA
      EOF))
                                        
(define-lex-abbrevs
  [letter (:or (:/ "a" "z") (:/ #\A #\Z) )]
  [digit (:/ #\0 #\9)]
  [identifier (:: (:or letter #\_) (:* (:or letter digit #\_)))])
                                     

(define sql-lexer
  (lexer-src-pos
   [(eof) 'EOF]
   [(:or #\tab #\space)
    (return-without-pos (sql-lexer input-port))]
   [(:or "+" "-") (token-ADDOP (string->symbol lexeme))]
   [(:or "*" "/") (token-MULOP (string->symbol lexeme))]
   [(:or "<" ">" "=" "!=" "<=" ">=") (token-COMP (string->symbol lexeme))]
   ["," 'COMMA]
   ["(" 'OP]
   [")" 'CP]
   ["and" 'AND]
   ["or" 'OR]
   [identifier (token-IDENTIFIER lexeme)]
   [(:: #\' (:* any-char) #\')
    (token-STR (substring lexeme 1 (- (string-length lexeme) 1)))]
   [(:+ digit) (token-NUM (string->number lexeme))]
   [(:: (:+ digit) #\. (:* digit)) (token-NUM (string->number lexeme))]))

(define arith-sym-to-fun (hash '= =
                                '< <
                                '<= <=
                                '> >
                                '>= >=
                                '!= (λ (v1 v2) (not (= v1 v2)))))

(define string-sym-to-fun (hash '= string=?
                                '< string<?
                                '<= string<=?
                                '> string>?
                                '>= string>=?
                                '!= (λ (v1 v2) (not (string=? v1 v2)))))

(struct atom (type is-id? val) #:transparent)
(struct exp (op type e1 e2) #:transparent)
(struct cond (cop e1 e2) #:transparent)
(struct clause (connector c1 c2) #:transparent)
(struct ast (clause-type root) #:transparent)

(define/contract (get-type e)
      (-> (or/c exp? atom?) symbol?)
      (if (exp? e)
          (exp-type e)
          (atom-type e)))

(define/contract (build-parser p-type)
  (-> (one-of/c 'where 'update) any/c)
  (λ (type-map [updatable null])
    (define (parse-cond cop e1 e2)
      (let ([type1 (get-type e1)]
            [type2 (get-type e2)])
        (if (eq? type1 type2)
            (case p-type
              [(where) (cond cop e1 e2)]
              [(update)
               (if (eq? cop '=)
                 (if (and (atom? e1) (atom-is-id? e1) (member (atom-val e1) updatable))                    
                     (cond cop e1 e2)
                     (error 'parser
                            "lhs ~a of assignment in update does not refer to updatable column" e1))
                 (error 'parser "illegal comparison ~a in update" cop))])
            (error 'parser
                   "type error comparing ~a to ~a" type1 type2))))
    (define (parse-binop op e1 e2)
      (let ([type1 (get-type e1)]
            [type2 (get-type e2)])
        (if (and (eq? type1 type2)
                 (or (eq? type1 'num) (eq? op '+)))
            (exp op type1 e1 e2)
            (error 'parser "illegal types ~a and ~a to binop ~a"
                   type1 type2 op))))
    (parser
     (src-pos)
     (start clause)
     (end EOF)
     (tokens value-tokens op-tokens)
     (error
      (λ (tok-ok? tok-name tok-value start-pos end-pos)
        (error 'where "Syntax error: unexpected token ~a (\"~a\") at ~a"
               tok-name tok-value (position-offset start-pos))))
     (precs (left COMMA)
            (left OR)
            (left AND)
            (left ADDOP)
            (left MULOP))
     (grammar
      (cond
        [(OP cond CP) $2]
        [(exp COMP exp) (parse-cond $2 $1 $3)])
      (clause
       [(cond) $1]
       [(clause COMMA clause) (if (eq? p-type 'where)
                                  (error 'parser
                                         "illegal token , for parser type where")
                                  (clause 'COMMA $1 $3))]
       [(clause AND clause) (if (eq? p-type 'where)
                                (clause 'AND $1 $3)
                                (error 'parser
                                       "illegal token and for parser type ~a" p-type))]
       [(clause OR clause) (if (eq? p-type 'where)
                               (clause 'OR $1 $3)
                               (error 'parser
                                      "illegal token or for parser type ~a" p-type))])   
      (atom
       [(STR) (atom 'str #f $1)]
       [(NUM) (atom 'num #f $1)]
       [(IDENTIFIER) (with-handlers ([exn:fail?
                                      (λ (e)
                                        (error 'parser
                                               "undefined identifier ~a" $1))])
                       (atom (hash-ref type-map $1) #t $1))]
       [(OP exp CP) $2])
      (exp
       [(atom) $1]
       [(exp ADDOP exp) (parse-binop $2 $1 $3)]
       [(exp MULOP exp) (parse-binop $2 $1 $3)])))))

(define (ast-to-string ast)
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (if (eq? (ast-clause-type ast) 'where)
           (format "(~a) ~a (~a)" (aux c1) connector (aux c2))
           (format "~a ~a ~a" (aux c1) connector (aux c2)))]
      [(cond cop e1 e2) (format "~a ~a ~a" (aux e1) cop (aux e2))]
      [(exp op type e1 e2) (format "~a ~a ~a" (aux e1) op (aux e2))]
      [(atom type is-id? val) (~a val)]))
  (aux (ast-root ast)))

(define (binop-sym-to-fun op type)
  (case op
    [(+) (if (eq? type 'str)
             string-append
             +)]
    [(-) -]
    [(*) *]
    [(/) /]
    [else error 'binop-sym-to-fun "invalid binop ~a" op]))

(define/contract (where-ast-to-fun ast)
  (-> (and/c ast? (λ (a) (eq? (ast-clause-type a) 'where))) procedure?)
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (case connector
         [(AND) (and (aux c1) (aux c2))]
         [(OR) (or (aux c1) (aux c2))]
         [else error 'where-ast-to-fun "invalid connector ~a for where clause" connector])]
      [(cond cop e1 e2) (let ([comp-fun (if (eq? (get-type e1) 'str)
                                           (hash-ref string-sym-to-fun cop)
                                           (hash-ref arith-sym-to-fun cop))])
                          (λ (r) (comp-fun ((aux e1) r) ((aux e2) r))))]
      [(exp op type e1 e2) (λ (r) ((binop-sym-to-fun op type) ((aux e1) r) ((aux e2) r)))]
      [(atom type is-id? val) (if is-id?
                                  (λ (r) (hash-ref r val))
                                  (λ (r) val))]))
  (aux (ast-root ast)))

(define/contract (update-ast-to-fun ast)
  (-> (and/c ast? (λ (a) (eq? (ast-clause-type a) 'update))) procedure?)
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (case connector
         [(COMMA) (λ (r) (append ((aux c1) r) ((aux c2) r)))]
         [else error 'update-ast-to-fun "invalid connector ~a for update clause" connector])]
      [(cond cop e1 e2) (λ (r) (list (cons (atom-val e1) ((aux e2) r))))]
      [(exp op type e1 e2) (λ (r) ((binop-sym-to-fun op type) ((aux e1) r) ((aux e2) r)))]
      [(atom type is-id? val) (if is-id?
                                  (λ (r) (hash-ref r val))
                                  (λ (r) val))]))
  (aux (ast-root ast)))

(define where-parser (build-parser 'where))
(define update-parser (build-parser 'update))

(define (lexer-thunk port)
  (port-count-lines! port)
  (λ () (sql-lexer port)))

(define (parse-update str type-map updatable)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) r)
        (let ([oip (open-input-string str)])
          (begin0
            ((update-parser type-map updatable) (lexer-thunk oip))
            (close-input-port oip)))))

(define (apply-update str rows updatable type-map)
  (let* ([update-ast (ast 'update (parse-update str type-map updatable))]
         [update-fun (update-ast-to-fun update-ast)]
         [row-fun (λ (row) (foldl (λ (replace h) (hash-set h (car replace) (cdr replace)))
                                  row (update-fun row)))])
    (map row-fun rows)))

(define (parse-where str type-map)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) #t)
        (let ([oip (open-input-string str)])
          (begin0
            (ast 'where ((where-parser type-map) (lexer-thunk oip)))
            (close-input-port oip)))))

(define tm (make-hash (list (cons "col1" 'num) (cons "col2" 'num))))
(define updatable (list "col1"))
(define r (make-immutable-hash (list (cons "col1" 2) (cons "col2" 5))))
(define w (parse-where "col1 < col2" tm))
