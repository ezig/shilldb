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

(define (check-types e1 t1 e2 t2)
    (and (eq? t1 (car e1)) (eq? t2 (car e2))))

(define (where-parser type-map)
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
     [(OP cond CP) $2]
     [(exp COMP exp) (if (check-types $1 'num $3 'num)
                         (λ (r) ((hash-ref arith-sym-to-func $2) ((cdr $1) r) ((cdr $3) r)))
                         (if (check-types $1 'str $3 'str)
                             (λ (r) ((hash-ref string-sym-to-func $2) ((cdr $1) r) ((cdr $3) r)))
                             (error 'where-parser
                             "type error comparing ~a to ~a" (car $1) (car $3))))])
    
    (cond
      [(basecond) $1]
      [(cond AND cond) (and $1 $3)]
      [(cond OR cond) (or $1 $3)])

    (atom
     [(STR) (cons 'str (λ (r) $1))]
     [(NUM) (cons 'num (λ (r) $1))]
     [(IDENTIFIER) (with-handlers ([exn:fail?
                                    (λ (e)
                                      (error 'where-parser
                                             "undefined identifier ~a" $1))])
                     (cons (hash-ref type-map $1) (λ (r) (hash-ref r $1))))]
     [(OP exp CP) $2])
    
    (exp
     [(atom) $1]
     [(exp + exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (+ ((cdr $1) r) ((cdr $3) r))))
                      (if (check-types $1 'str $3 'str)
                          (cons 'str (λ (r) (string-append ((cdr $1) r) ((cdr $3) r))))
                          (error 'where-parser
                             "type error adding ~a to ~a" (car $1) (car $3))))]
     [(exp - exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (- ((cdr $1) r) ((cdr $3) r))))
                      (error 'where-parser
                             "type error subtracting ~a from ~a" (car $1) (car $3)))]
     [(exp * exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (* ((cdr $1) r) ((cdr $3) r))))
                      (error 'where-parser
                             "type error multiplying ~a and ~a" (car $1) (car $3)))]
     [(exp / exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (/ ((cdr $1) r) ((cdr $3) r))))
                      (error 'where-parser
                             "type error dividing ~a by ~a" (car $1) (car $3)))]))))

(define (update-parser updatable type-map)
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
      [(IDENTIFIER = exp) (if (member $1 updatable)
                              (if (equal? (car $3) (hash-ref type-map $1))
                                  (λ (r) (list (cons $1 ((cdr $3) r))))
                                  (error 'update-parser
                                         "type error assigning ~a exp to column with type ~a"
                                         (car $3) (hash-ref type-map $1)))
                              (error 'update-parser
                                     "non-updatable column \"~a\" on LHS of update" $1))]
      [(set-statement COMMA set-statement) (λ (r) (append ($1 r) ($3 r)))])

    (atom
     [(STR) (cons 'str (λ (r) $1))]
     [(NUM) (cons 'num (λ (r) $1))]
     [(IDENTIFIER) (with-handlers ([exn:fail?
                                    (λ (e)
                                      (error 'where-parser
                                             "undefined identifier ~a" $1))])
                     (cons (hash-ref type-map $1) (λ (r) (hash-ref r $1))))]
     [(OP exp CP) $2])
    
    (exp
     [(atom) $1]
     [(exp + exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (+ ((cdr $1) r) ((cdr $3) r))))
                      (if (check-types $1 'str $3 'str)
                          (cons 'str (λ (r) (string-append ((cdr $1) r) ((cdr $3) r))))
                          (error 'where-parser
                             "type error adding ~a to ~a" (car $1) (car $3))))]
     [(exp - exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (- ((cdr $1) r) ((cdr $3) r))))
                      (error 'where-parser
                             "type error subtracting ~a from ~a" (car $1) (car $3)))]
     [(exp * exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (* ((cdr $1) r) ((cdr $3) r))))
                      (error 'where-parser
                             "type error multiplying ~a and ~a" (car $1) (car $3)))]
     [(exp / exp) (if (check-types $1 'num $3 'num)
                      (cons 'num (λ (r) (/ ((cdr $1) r) ((cdr $3) r))))
                      (error 'where-parser
                             "type error dividing ~a by ~a" (car $1) (car $3)))]))))

(define (lexer-thunk lexer port)
  (port-count-lines! port)
  (λ () (lexer port)))

(define (parse-update str updatable type-map)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) r)
        (let ([oip (open-input-string str)])
          (begin0
            ((update-parser updatable type-map) (lexer-thunk update-lexer oip))
            (close-input-port oip)))))

(define (apply-update str rows updatable type-map)
  (let* ([update-fun (parse-update str updatable type-map)]
         [row-fun (λ (row) (foldl (λ (replace h) (hash-set h (car replace) (cdr replace)))
                                  row (update-fun row)))])
    (map row-fun rows)))

(define (parse-where str type-map)
    (if (or (null? str) (not (non-empty-string? str)))
        (λ (r) #t)
        (let ([oip (open-input-string str)])
          (begin0
            ((where-parser type-map)(lexer-thunk where-lexer oip))
            (close-input-port oip)))))
