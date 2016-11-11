#lang racket

(provide parse-where)
(provide apply-update)

(require parser-tools/yacc
         parser-tools/lex
         (prefix-in : parser-tools/lex-sre))

(define-tokens value-tokens (IDENTIFIER NUM STR COMP))
(define-empty-tokens op-tokens
  (OP CP            ; ( )
      AND              ; and
      OR               ; or
      + - * /
      =
      COMMA
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

(define update-lexer
  (lexer-src-pos
   [(eof) 'EOF]
   [(:or #\tab #\space)
    (return-without-pos (update-lexer input-port))]
   [(:or "+" "-" "*" "/") (string->symbol lexeme)]
   ["," 'COMMA]
   ["=" '=]
   ["(" 'OP]
   [")" 'CP]
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
     [(num-exp COMP num-exp) (λ (r) ((hash-ref arith-sym-to-func $2) ($1 r) ($3 r)))])
    
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

(define update-parser
  (parser
   (src-pos)
   (start set-statement)
   (end EOF)
   (tokens value-tokens op-tokens)
   (error
    (λ (tok-ok? tok-name tok-value start-pos end-pos)
      (parse-where "(")(error 'where "Syntax error: unexpected token ~a (\"~a\") at ~a"
             tok-name tok-value (position-offset start-pos))))
   (precs (left COMMA)
          (left + -)
          (left * /))
   (grammar    
    (set-statement
      [(IDENTIFIER = exp) (λ (r) (list (cons $1 ($3 r))))]
      [(set-statement COMMA set-statement) (λ (r) (append ($1 r) ($3 r)))])

    (atom
     [(STR) (λ (r) $1)]
     [(NUM) (λ (r) $1)]
     [(IDENTIFIER) (λ (r) (hash-ref r $1))]
     [(OP exp CP) $2])
    
    (exp
     [(atom) $1]
     [(exp + exp) (λ (r) (+ ($1 r) ($3 r)))]
     [(exp - exp) (λ (r) (- ($1 r) ($3 r)))]
     [(exp * exp) (λ (r) (* ($1 r) ($3 r)))]
     [(exp / exp) (λ (r) (/ ($1 r) ($3 r)))]))))

(define (lexer-thunk lexer port)
  (port-count-lines! port)
  (λ () (lexer port)))

(define (parse-update str)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) r)
        (let ([oip (open-input-string str)])
          (begin0
            (update-parser (lexer-thunk update-lexer oip))
            (close-input-port oip)))))

(define (apply-update str rows)
  (let* ([update-fun (parse-update str)]
         [row-fun (λ (row) (foldl (λ (replace h) (hash-set h (car replace) (cdr replace)))
                                  row (update-fun row)))])
    (map row-fun rows)))

(define (parse-where str)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) #t)
        (let ([oip (open-input-string str)])
          (begin0
            (where-parser (lexer-thunk where-lexer oip))
            (close-input-port oip)))))
