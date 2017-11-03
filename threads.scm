;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; parallel map and related utilities
;;
;; pmap does not play nicely with too many tcp connections,
;; so pmap-batch can be used to limit the number of concurrent
;; connections. This also could be done with a pool.

(use srfi-1 srfi-18 http-client)

(define (pvec #!rest thunks)
  (let* ((len (length thunks))
         (result (make-vector len #f))
         (remaining len)
         (result-mutex (make-mutex))
         (done-mutex (make-mutex)))
    (letrec ((children
              (map (lambda (thunk k)
                     (make-thread
                      (lambda ()
                        (let ((x (thunk)))
                          (mutex-lock! result-mutex #f #f)
                          (vector-set! result k x)
                          (set! remaining (- remaining 1))
                          (mutex-unlock! result-mutex)
                          (when (= remaining 0)
                            (mutex-unlock! done-mutex))))))
                   thunks (list-tabulate len values))))
      (mutex-lock! done-mutex #f #f)
      (map thread-start! children)
      (mutex-lock! done-mutex #f #f)
      result)))

(define (take-max lst n)
  (if (or (null? lst) (= n 0))
      '()
      (cons (car lst) (take-max (cdr lst) (- n 1)))))

(define (drop-max lst n)
  (if (or (null? lst) (= n 0))
      lst
      (drop-max (cdr lst) (- n 1))))

(define (pvec-batch batch-size thunks)
  (let* ((len (length thunks))
         (result (make-vector len #f))
         (remaining len)
         (result-mutex (make-mutex))         
         (done-mutex (make-mutex)))
    (let loop ((thunks thunks)
               (batch 0))
      (if (null? thunks)
          result
          (letrec ((batch-remaining (min batch-size (length thunks)))
                   (batch-done-mutex (make-mutex))
                   (children
                    (map (lambda (thunk k)
                           (make-thread
                            (lambda ()
                              (let ((x (thunk)))
                                (mutex-lock! result-mutex #f #f)
                                (vector-set! result k x)
                                (set! remaining (- remaining 1))
                                (set! batch-remaining (- batch-remaining 1))
                                (mutex-unlock! result-mutex)
                                (when (= batch-remaining 0)
                                  (mutex-unlock! batch-done-mutex)) ))))
                         (take-max thunks batch-size)
                         (list-tabulate len
                                        (lambda (x) (+ x (* batch batch-size)))))))
            (mutex-lock! batch-done-mutex #f #f)
            (map thread-start! children)
            (mutex-lock! batch-done-mutex #f #F)
            (loop (drop-max thunks batch-size) (+ batch 1)))))))

(define (pmap fn #!rest lists)
  (vector->list
   (apply pvec
    (apply map
           (lambda (e) (lambda () (fn e)))
           lists))))

(define (call thunk) (thunk))

(define-syntax plet
  (syntax-rules ()
    ((_ ((var exp) ...) body ...)
     (let ((bindings (pmap call (list (lambda () exp) ...))))
       (match-let (((var ...) bindings))
         body ...)))))

(define-syntax plet-if
  (syntax-rules ()
    ((_ test ((var exp) ...) body ...)
     (if test
         (plet ((var exp) ...) body ...)
         (match-let ((var exp) ...) body ...)))))

(define (pmap-batch batch-size fn #!rest lists)
  (vector->list
   (pvec-batch batch-size
     (apply map
            (lambda (e) (lambda () (fn e)))
            lists))))

(define (pmap-vec fn #!rest lists)
   (apply pvec
    (apply map
           (lambda (e) (lambda () (fn e)))
           lists)))
