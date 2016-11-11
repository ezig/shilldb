#lang racket

(provide parse-where)

(require parser-tools/yacc
         parser-tools/lex
         (prefix-in : parser-tools/lex-sre))

(define-tokens value-tokens (IDENTIFIER NUM STR COMP))
(define-empty-tokens op-tokens
  (OP CP            ; ( )
      AND              ; and
      OR               ; or
      + - * /
      EOF))
                                        
(define-lex-abbrevs
  [letter (:or (:/ "a" "z") (:/ #\A #\Z) )]
  [digit (:/ #\0 #\9)]
  [identifier (:: (:or letter #\_) (:* (:or letter digit #\_)))])
                                     

(define where-lexer
  (lexer-src-pos
   [(eof) 'EOF]
   [(:or #\tab #\space)
    (return-without-pos (where-lexer input-port))]
   [(:or "+" "-" "*" "/") (string->symbol lexeme)]
   [(:or "<" ">" "=" "!=" "<=" ">=") (token-COMP (string->symbol lexeme))]
   ["(" 'OP]
   [")" 'CP]
   ["and" 'AND]
   ["or" 'OR]
   [identifier (token-IDENTIFIER lexeme)]
   [(:: #\' (:* any-char) #\')
    (token-STR (substring lexeme 1 (- (string-length lexeme) 1)))]
   [(:+ digit) (token-NUM (string->number lexeme))]
   [(:: (:+ digit) #\. (:* digit)) (token-NUM (string->number lexeme))]))

(define arith-sym-to-func (hash '= =
                                '< <
                                '<= <=
                                '> >
                                '>= >=
                                '!= (λ (v1 v2) (not (= v1 v2)))))

(define string-sym-to-func (hash '= string=?
                                '< string<?
                                '<= string<=?
                                '> string>?
                                '>= string>=?
                                '!= (λ (v1 v2) (not (string=? v1 v2)))))

(define where-parser
  (parser
   (src-pos)
   (start cond)
   (end EOF)
   (tokens value-tokens op-tokens)
   (error
    (λ (tok-ok? tok-name tok-value start-pos end-pos)
      (parse-where "(")(error 'where "Syntax error: unexpected token ~a (\"~a\") at ~a"
             tok-name tok-value (position-offset start-pos))))
   (precs (left OR)
          (left AND)
          (left + -)
          (left * /))
   (grammar
    (basecond
     [(OP cond CP ) $2]
     [(IDENTIFIER COMP num-exp) (λ (r) ((hash-ref arith-sym-to-func $2) (hash-ref r $1) ($3 r)))]
     [(IDENTIFIER COMP str-exp) (λ (r) ((hash-ref string-sym-to-func $2) (hash-ref r $1) ($3 r)))])
    
    (cond
      [(basecond) $1]
      [(cond AND cond) (and $1 $3)]
      [(cond OR cond) (or $1 $3)])

    (num-atom
     [(NUM) (λ (r) $1)]
     [(IDENTIFIER) (λ (r) (hash-ref r $1))]
     [(OP num-exp CP) $2])
    
    (num-exp
     [(num-atom) $1]
     [(num-exp + num-exp) (λ (r) (+ ($1 r) ($3 r)))]
     [(num-exp - num-exp) (λ (r) (- ($1 r) ($3 r)))]
     [(num-exp * num-exp) (λ (r) (* ($1 r) ($3 r)))]
     [(num-exp / num-exp) (λ (r) (/ ($1 r) ($3 r)))])

    (str-exp
     [(STR) (λ (r) $1)]))))

(define (lexer-thunk port)
  (port-count-lines! port)
  (λ () (where-lexer port)))

(define (parse-where str)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) #t)
        (let ([oip (open-input-string str)])
          (begin0
            (where-parser (lexer-thunk oip))
            (close-input-port oip)))))