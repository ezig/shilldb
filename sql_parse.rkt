#lang racket

(provide parse-where)
(provide restrict-where)
(provide empty-where)
(provide where-to-fun)
(provide apply-update)

(require "util.rkt")
(require parser-tools/yacc
         parser-tools/lex
         (prefix-in : parser-tools/lex-sre))

(define-tokens value-tokens (IDENTIFIER NUM STR COMP ADDOP MULOP))
(define-empty-tokens op-tokens
  (OP CP            ; ( )
      AND              ; and
      OR               ; or
      COMMA            ; ,
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
       [(clause COMMA clause) (if (not (eq? p-type 'update))
                                  (error 'parser
                                         "illegal token , for parser type where")
                                  (clause 'COMMA $1 $3))]
       [(clause AND clause) (if (not (eq? p-type 'update))
                                (clause 'AND $1 $3)
                                (error 'parser
                                       "illegal token and for parser type ~a" p-type))]
       [(clause OR clause) (if (not (eq? p-type 'update))
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


(define (binop-sym-to-fun op type)
  (case op
    [(+) (if (eq? type 'str)
             string-append
             +)]
    [(-) -]
    [(*) *]
    [(/) /]
    [else (error 'binop-sym-to-fun "invalid binop ~a" op)]))

(define (comp-sym-to-fun op type)
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
  (if (eq? type 'str)
      (hash-ref string-sym-to-fun op)
      (hash-ref arith-sym-to-fun op)))

(define/contract (ast-to-fun ast)
  (-> ast? procedure?)
  (define (aux t)
    (match t
      [(clause connector c1 c2)
       (case connector
         [(AND) (and (aux c1) (aux c2))]
         [(OR) (or (aux c1) (aux c2))]
         [(COMMA) (λ (r) (append ((aux c1) r) ((aux c2) r)))]
         [else (error 'ast-to-fun "invalid connector ~a" connector)])]
      [(cond cop e1 e2)
       (case (ast-clause-type ast)
         [(where) (λ (r) ((comp-sym-to-fun cop (get-type e1)) ((aux e1) r) ((aux e2) r)))]
         [(update) (λ (r) (list (cons (atom-val e1) ((aux e2) r))))]
         [else (error 'ast-to-fun "invalid clause type ~a" (ast-clause-type ast))])]
      [(exp op type e1 e2) (λ (r) ((binop-sym-to-fun op type) ((aux e1) r) ((aux e2) r)))]
      [(atom type is-id? val) (if is-id?
                                  (λ (r) (hash-ref r val))
                                  (λ (r) val))]))
  (let ([root (ast-root ast)])
    (if (null? root)
        (case (ast-clause-type ast)
          [(where) (λ (r) #t)]
          [(update) (λ (r) r)]
          [else (error 'ast-to-fun "invalid clause type ~a" (ast-clause-type ast))])
        (aux root))))

(define (lexer-thunk port)
  (port-count-lines! port)
  (λ () (sql-lexer port)))

(define (parse-clause str clause-type p-args)
     (if (or (null? str) (not (non-empty-string? str)))
        (ast 'where null)
        (let ([oip (open-input-string str)]
              [parser (case clause-type
                        [(where) where-parser]
                        [(update) update-parser]
                        [else (error 'parse-clause "unsupported clause type ~a" clause-type)])])
          (begin0
            (ast clause-type ((apply parser p-args) (lexer-thunk oip)))
            (close-input-port oip)))))

(define where-parser (build-parser 'where))
(define update-parser (build-parser 'update))

(define (parse-where str type-map)
  (parse-clause str 'where (list type-map)))
(define (parse-update str type-map updatable)
  (parse-clause str 'update (list type-map updatable)))

(define empty-where
  (ast 'where null))

(define/contract (restrict-where where-ast restrict-str type-map)
  (-> ast? string? hash? ast?)
  (let ([restrict-ast (parse-where restrict-str type-map)]
        [old-root (ast-root where-ast)])
    (if (null? old-root)
        restrict-ast
        (ast 'where (clause 'AND old-root (ast-root restrict-ast))))))

(define/contract (where-to-fun ast)
  (-> (and/c ast? (λ (a) (eq? (ast-clause-type a) 'where))) procedure?)
  (ast-to-fun ast))

(define (apply-update str rows updatable type-map)
  (let* ([update-ast (parse-update str type-map updatable)]
         [update-fun (ast-to-fun update-ast)]
         [row-fun (λ (row) (foldl (λ (replace h) (hash-set h (car replace) (cdr replace)))
                                  row (update-fun row)))])
    (map row-fun rows)))

(define tm (make-hash (list (cons "col1" 'num) (cons "col2" 'num))))
; (define updatable (list "col1"))
; (define r (make-immutable-hash (list (cons "col1" 2) (cons "col2" 5))))
; (define w (parse-where "col1 < col2" tm))
