#lang racket

(require "shilldb.rkt")

; Function that displays employee directory to an employee with id `id`
; Maybe allows employee to interact with directory, but employee can only update
; their own record and should only be able to edit their name
(define/contract (display-employees id emp)
  (->i ([id integer?]
       [emp (id) (view/c  #:fetch (list "fetch" #t) #:update (list "update" #t any/c any/c (λ (v) (where v (format "id = ~a" id)))))])
       [result any/c])
       (update emp "name = 'Jerry'"))

; It is fine to look at the employees table, but you can only look at the salaries of employees in your department (department 0)
; After you join the salaries on the employees in your department, running a fetch on the view is fine
(define (build-emp-salary-pair emp sal)
  (define/contract emp/c (view/c #:fetch (list "fetch" #t) #:where (list "where" #t) #:join (list "join" #t (λ (v) (where v "department = 0")))) emp)
  (define/contract sal/c (view/c #:join (list "join" #t values
                                              (view/c #:fetch (list "fetch" #t)
                                                      #:where (list "where" #t)))) sal)
  (define/contract vp/c (view-pair/c #:join (list "join" #t (λ (v) (where v "lhs_id = rhs_empid")))) (build-view-pair emp/c sal/c))
  vp/c)

(define (reveal-salaries vp)
  (fetch (pjoin vp "")))
  
(module+ test
  (define emp (open-view "examples/employees/employees.db" "employees"))
  (define sal (open-view "examples/employees/employees.db" "salaries"))
  (display-employees 0 (open-view "examples/employees/employees.db" "employees"))
  (reveal-salaries (build-emp-salary-pair emp sal))
  )