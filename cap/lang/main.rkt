#lang racket/base

(require shilldb/private/out
         racket/bool
         racket/bytes
         racket/contract
         racket/dict
         racket/exn
         racket/extflonum
         racket/fixnum
         racket/flonum
         racket/function
         racket/hash
         racket/list
         racket/match
         racket/math
         racket/set
         racket/sequence
         racket/stream
         racket/string
         racket/undefined)

;; Adapted from Moore

; Core forms
(provide #%module-begin
         #%app #%plain-app #%expression #%top
         quote #%datum
         lambda λ case-lambda #%plain-lambda
         begin begin0 define
         let let* letrec let-values let*-values letrec-values)

(provide displayln)

; ShillDB forms
(provide (except-out (all-from-out shilldb/private/out)
                     open-view))

(provide (rename-out [shilldb-provide provide])
         (rename-out [shilldb-require require]))

; Control
(provide if else cond => and or case when unless
         for for/list for/and for/or for/sum for/product for/lists for/first for/last for/fold
         for/hash for* for*/list for*/and for*/or for*/sum for*/product for*/lists for*/first
         for*/last for*/fold for*/hash do)

(provide match match* match/values define/match match-lambda match-lambda* match-lambda** match-let
         match-let* match-let-values match-let*-values match-letrec match-letrec-values match-define
         match-define-values)

(provide values call-with-values)

(provide raise error raise-user-error raise-argument-error raise-result-error raise-arguments-error
         raise-range-error raise-type-error raise-mismatch-error raise-arity-error raise-syntax-error
         call-with-exception-handler with-handlers with-handlers* exn->string
         (struct-out exn) (struct-out exn:fail) (struct-out exn:fail:contract) (struct-out exn:fail:contract:arity)
         (struct-out exn:fail:contract:divide-by-zero) (struct-out exn:fail:contract:non-fixnum-result)
         (struct-out exn:fail:out-of-memory) (struct-out exn:fail:unsupported) (struct-out exn:fail:user)
         (struct-out exn:break) (struct-out exn:break:hang-up) (struct-out exn:break:terminate))

(provide exit)

; REPL
(provide #%top-interaction #%top)

; Primitive operations
(provide equal? void? void undefined format
         ; booleans
         boolean? not true false false? nand nor implies xor
         ; numbers
         number? complex? real? rational? integer? exact-integer? exact-nonnegative-integer?
         exact-positive-integer? inexact-real? fixnum? flonum? double-flonum? single-flonum?
         zero? positive? negative? even? odd? exact? inexact? inexact->exact exact->inexact
         real->single-flonum real->double-flonum
         + - * / quotient remainder quotient/remainder modulo add1 sub1 abs max min gcd lcm
         round floor ceiling truncate numerator denominator rationalize
         = < <= > >= sqrt integer-sqrt integer-sqrt/remainder expt exp log
         sin cos tan asin acos atan make-rectangular make-polar real-part imag-part
         magnitude angle bitwise-ior bitwise-and bitwise-xor bitwise-not bitwise-bit-set?
         bitwise-bit-field arithmetic-shift integer-length
         number->string string->number real->decimal-string integer-bytes->integer
         integer->integer-bytes floating-point-bytes->real real->floating-point-bytes
         system-big-endian? pi pi.f degrees->radians radians->degrees sqr sgn conjugate
         sinh cosh tanh exact-round exact-floor exact-ceiling exact-truncate order-of-magnitude
         nan? infinite? fl+ fl- fl* fl/ flabs fl= fl< fl> fl<= fl>= flmin flmax flround flfloor
         flceiling fltruncate flsin flcos fltan flasin flacos flatan fllog flexp flsqrt flexpt
         ->fl fl->exact-integer make-flrectangular flreal-part flimag-part flrandom fx+ fx- fx*
         fxquotient fxremainder fxmodulo fxabs fxand fxior fxxor fxnot fxlshift fxrshift fx= fx<
         fx> fx<= fx>= fxmin fxmax fx->fl fl->fx extflonum? extflonum-available? extfl+ extfl-
         extfl* extfl/ extflabs extfl= extfl< extfl> extfl<= extfl>= extflmin extflmax extflround
         extflfloor extflceiling extfltruncate extflsin extflcos extfltan extflasin extflacos
         extflatan extfllog extflexp extflsqrt extflexpt ->extfl extfl->exact-integer real->extfl
         extfl->exact extfl->inexact pi.t
         ; strings
         string? string make-string list->string string-length string-ref substring string-append
         string->list build-string string=? string<? string<=? string>? string>=? string-ci=?
         string-ci<? string-ci<=? string-ci>? string-ci>=? string-upcase string-downcase
         string-titlecase string-foldcase string-normalize-nfd string-normalize-nfkd
         string-normalize-nfc string-normalize-nfkc string-locale=? string-locale<? string-locale>?
         string-locale-ci=? string-locale-ci<? string-locale-ci>? string-locale-upcase
         string-locale-downcase string-append* string-join string-normalize-spaces string-replace
         string-split string-trim non-empty-string? string-contains? string-prefix? string-suffix?
         ; byte strings
         bytes? make-bytes bytes byte? bytes-length bytes-ref subbytes bytes-copy bytes-append
         bytes->list list->bytes bytes=? bytes<? bytes>? bytes->string/utf-8 bytes->string/locale
         bytes->string/latin-1 string->bytes/utf-8 string->bytes/locale string->bytes/latin-1
         string-utf-8-length bytes-utf-8-length bytes-utf-8-ref bytes-utf-8-index locale-string-encoding
         bytes-append* bytes-join
         ; characters
         char? char->integer integer->char char-utf-8-length char=? char<? char<=? char>? char>=?
         char-ci=? char-ci<? char-ci<=? char-ci>? char-ci>=? char-alphabetic? char-lower-case?
         char-upper-case? char-title-case? char-numeric? char-symbolic? char-punctuation? char-graphic?
         char-whitespace? char-blank? char-iso-control? char-general-category make-known-char-range-list 
         char-upcase char-downcase char-titlecase char-foldcase
         ; symbols
         symbol? symbol-interned? symbol-unreadable? symbol->string string->symbol string->uninterned-symbol
         string->unreadable-symbol gensym symbol<?
         ; regular expressions
         regexp? pregexp? byte-regexp? byte-pregexp? regexp pregexp byte-regexp byte-pregexp regexp-quote
         regexp-max-lookbehind regexp-match regexp-match* regexp-try-match regexp-match-positions
         regexp-match-positions* regexp-match? regexp-match-exact? regexp-split regexp-replace
         regexp-replace* regexp-replaces regexp-replace-quote
         ; keywords
         keyword? keyword->string string->keyword keyword<?)

; Lists
(provide pair? null? cons car cdr null list? list list* build-list length list-ref list-tail append reverse
         map andmap ormap for-each foldl foldr filter remove remq remv remove* remq* remv* sort member memf
         findf assoc assf caar cadr cdar cddr caaar caadr cadar caddr cdaar cdadr cddar cdddr caaaar caaadr
         caadar caaddr cadaar cadadr caddar cadddr cdaaar cdaadr cdadar cdaddr cddaar cddadr cdddar cddddr
         empty cons? empty? first rest second third fourth fifth sixth seventh eighth ninth tenth last
         last-pair make-list list-update list-set take drop split-at takef dropf splitf-at take-right drop-right
         split-at-right takef-right dropf-right splitf-at-right list-prefix? take-common-prefix drop-common-prefix
         split-common-prefix add-between append* flatten check-duplicates remove-duplicates filter-map count
         partition range append-map filter-not combinations in-combinations permutations in-permutations
         argmin argmax group-by cartesian-product remf remf*)

; hashes
(provide hash? hash make-hash hash-set hash-set* hash-ref hash-has-key? hash-update hash-remove hash-clear
         hash-map hash-keys hash-values hash->list hash-for-each hash-count hash-empty? hash-iterate-first
         hash-iterate-next hash-iterate-key hash-iterate-value hash-iterate-pair hash-iterate-key+value
         hash-union)

; sequences
(provide sequence? in-range in-naturals in-list in-string in-bytes in-lines in-bytes-lines in-hash in-hash-keys
         in-hash-values in-hash-pairs in-value in-indexed in-sequences in-cycle in-parallel in-values-sequence
         in-values*-sequence stop-before stop-after sequence->stream sequence-generate* empty-sequence
         sequence->list sequence-length sequence-ref sequence-tail sequence-append sequence-map sequence-andmap
         sequence-ormap sequence-for-each sequence-fold sequence-count sequence-filter sequence-add-between
         sequence/c)

; streams
(provide stream? stream-empty? stream-first stream-rest stream-cons stream stream* in-stream empty-stream
         stream->list stream-length stream-ref stream-tail stream-append stream-map stream-andmap stream-ormap
         stream-for-each stream-fold stream-count stream-filter stream-add-between for/stream for*/stream
         stream/c)

; dicts
(provide dict? dict-ref dict-set dict-remove dict-iterate-first dict-iterate-next dict-iterate-key
         dict-iterate-value dict-has-key? dict-set* dict-update dict-map dict-for-each dict-empty?
         dict-count dict-copy dict-clear dict-keys dict-values dict->list in-dict in-dict-keys in-dict-values
         in-dict-pairs dict-key-contract dict-value-contract dict-iter-contract)

; sets
(provide set? set list->set for/set for*/set (rename-out [in-immutable-set in-set]) generic-set? set/c
         set-member? set-add set-remove set-empty? set-count set-first set-rest set->stream set-copy
         set-clear set-union set-intersect set-subtract set-symmetric-difference set=? subset? proper-subset?
         set->list set-map set-for-each)

; procedures
(provide procedure? apply compose compose1 keyword-apply procedure-arity? procedure-arity-includes?
         procedure-reduce-arity procedure-keywords procedure-result-arity make-keyword-procedure
         procedure-reduce-keyword-arity arity-at-least identity const thunk thunk* negate conjoin disjoin
         curry curryr normalized-arity? normalize-arity arity=? arity-includes?)

; contracts
;;;;;;;;;;;;

(provide define/contract)

; data structure contracts
(provide any/c none/c or/c first-or/c and/c not/c =/c </c >/c <=/c >=/c between/c real-in integer-in char-in
         natural-number/c string-len/c false/c printable/c one-of/c symbols listof non-empty-listof list*of
         cons/c cons/dc list/c *list/c procedure-arity-includes/c hash/c hash/dc flat-rec-contract
         flat-murec-contract any suggest/c if/c failure-result/c)

; function contracts
(provide -> ->* ->i ->d case-> dynamic->* unconstrained-domain-> predicate/c unsupplied-arg?)

; utilities
(provide contract? flat-contract? list-contract? has-contract?)
