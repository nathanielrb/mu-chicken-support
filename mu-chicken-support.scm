;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utilities for mu-chicken-template,
;; a mu-semtech Docker template for Chicken Scheme.

(module mu-chicken-support *

(import chicken scheme extras data-structures srfi-1 spiffy ports matchable srfi-69 posix)

(reexport sparql-query spiffy matchable medea)

(use sparql-query spiffy spiffy-request-vars
     cjson medea uri-common intarweb irregex srfi-13 srfi-18 matchable)

(include "threads.scm")

(define *handlers* (make-parameter '()))

(define (generate-uuid)
  (with-input-from-pipe "uuidgen" (lambda () (read-line))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Configuration parameters
(define (string->boolean val)
  (match val
    ((or #t "true" "True" "TRUE") #t)
    ((or #f "false" "False" "FALSE") #f)
    (_ val)))

(define (config-param env_name #!optional default converter)
  (let ((converter (or converter
                       (cond ((symbol? default) string->symbol)
                             ((number? default) string->number)
                             ((boolean? default) string->boolean)
                             (else values))))
        (env_val (get-environment-variable env_name)))
  (make-parameter
   (if env_val (converter env_val)
       default))))

(define *namespace-definitions* (config-param "MU_NAMESPACES" ""))

(define *port* (config-param "PORT" 80))

(define *swank-port* (config-param "SWANK_PORT" 4005))

(define *config-file* (make-parameter "./config.yaml"))

(define *pass-headers* (make-parameter '(mu-session-id mu-call-id)))

(debug-log
 (if (feature? 'docker)
     "/logs/debug.log"
     "debug.log"))

(access-log
 (if (feature? 'docker)
     "/logs/access.log"
     "access.log"))

(error-log
 (if (feature? 'docker)
     (current-error-port)
     "error.log"))

(define *message-logging?*
  (config-param "MESSAGE_LOGGING" #t))

(define (log-message str #!rest args)
  (when (*message-logging?*)
    (apply format (current-error-port) str args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Caching utilities
(define-syntax hit-property-cache
  (syntax-rules ()
    ((hit-property-cache sym prop body)
     (or (get sym prop)
         (put! sym prop body)))))

(define-syntax hit-hashed-cache
  (syntax-rules ()
    ((hit-hashed-cache cache key body)
     (or (hash-table-ref/default cache key #f)
         (begin
           (hash-table-set! cache key body)
           (hash-table-ref cache key))))))
    
(define (clear-hashed-cache! cache key)
  (hash-table-delete! cache key))
     
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rest Calls
;;
;; Example:
;;
;; (define (bobby bindings)
;;   '((name . "bobby")))
;;
;; (*handlers* `((GET ("bobby") ,bobby)))
;;
;; Alternative:
;;
;; (define person
;;   (rest-call (name)
;;     `((name . ,name))))
;;
;; (define-rest-call 'GET '("person" name) person)
;;
;; (vhost-map `((".*" . ,handle-app) ))
;;
;; (start-server port: 4028)

(define (lookup-handler method full-path handlers)
  (if (null? handlers)
      (values #f #f)
      (let* ((handler-spec (car handlers))
            (handler-method (car handler-spec)))
        (if (not (equal? handler-method method))
            (lookup-handler method full-path (cdr handlers)))
        (let loop ((handler (cadr handler-spec))
                   (path full-path)
                   (bindings '()))
          (cond ((and (null? handler) (null? path))
                 (values (caddr handler-spec) bindings))
                ((or (null? handler) (null? path) (not (equal? method handler-method)))
                 (lookup-handler method full-path (cdr handlers)))
                ((and (string? (car handler))
                      (equal? (car path) (car handler)))
                 (loop (cdr handler) (cdr path) bindings))
                ((symbol? (car handler))
                 (loop (cdr handler) (cdr path)
                       (cons (cons (car handler) (car path))
                             bindings)))
                (else
                 (lookup-handler method full-path (cdr handlers))))))))

(define (read-request-json)
  (let* ((content-length (header 'content-length))
	 (body (and content-length (read-string content-length (request-port (current-request))))))
    (and body (string->json body))))

(define (read-request-body)
  (let* ((content-length (header 'content-length)))
    (and content-length (read-string content-length (request-port (current-request))))))

(define mu-headers (make-parameter '()))

(define (errors-object errors)
  `((errors . ,(list->vector errors))))

(define (error-object status title detail)
  `((status . ,status)
    (title . ,title)
    (detail . ,detail)))

(define (single-error status title detail)
  (errors-object (list (error-object status title detail))))

(define (send-error status title detail)
  (send-response status: 'internal-server-error
                 body: (json->string
                        (single-error status title detail))))
                         
(define *request-headers* (make-parameter #f))

(define (header h)
  (and (*request-headers*)
       (header-value h (*request-headers*))))

(define (pass-headers)
  (filter 
   values
   (map (lambda (h) 
          (let ((v (header h)))
            (and v (list h v))))
        (*pass-headers*))))

(define (handle-app continue)
  (let ((uri (request-uri (current-request)))
        (method (request-method (current-request))))
    (let-values (((handler bindings) (lookup-handler method (cdr (uri-path uri)) (*handlers*))))
      (if handler
          (parameterize ((*request-headers* (request-headers (current-request))))
            (let ((body (handler bindings)))
              (send-response status: 'ok
                             body: (if (string? body) body
                                       (json->string body))
                             headers: (append (mu-headers)
                                              (pass-headers)
                                              (if (string? body) '()
                                                  '((content-type "application/json")))))))
      (send-response status: 'not-found 
                     body: (json->string
                            (single-error 404 "Not Found" "Request not found")))))))

(define (define-rest-call method path proc)
  (*handlers* (cons `(,method ,path ,proc) (*handlers*))))

(define-syntax rest-call
  (syntax-rules ()
    ((rest_call (vars ...) body ...)
     (lambda (bindings)
       (let ((vars (alist-ref (quote vars) bindings)) ...)
	   body ...)))))

(*handlers* `((GET (/ "test") ,(lambda (bindings)
				 `((message . "Hello, Open World."))))))

(vhost-map `((".*" . ,handle-app) ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Data Formats
(define (json-api-data data)
  (let ((data-vec (if (vector? data) data (list->vector data))))
    (and data (not (null? data))
         `((data . ,data-vec)))))

(define (json-api-relationship relation relationship)
  (and relationship (not (null? relationship))
       `((,relation . ,relationship))))

(define-syntax splice-when
  (syntax-rules ()
    ((splice-when body)
     (let ((body-val body))
       (splice-when body-val body-val)))
    ((splice-when test body)
     (if (or (not test) (null? test)) '() body))))

(define (jkv-when key val)
  (splice-when val `((,key . ,val))))

(define (json-api-object id uri type #!key attributes relationships links)
  `((id . ,id)
    (type . ,type)
    ,@(splice-when uri `((@id . ,uri)))
    ,@(splice-when attributes `((attributes . ,attributes)))
    ,@(splice-when relationships `((relationships . ,relationships)))
    ,@(splice-when links `((links . ,links)))))

(define (json-ld-object id type #!optional properties #!key context)
  `((@id . ,id)
     (@type . ,type)
     ,@(splice-when properties)
     ,@(splice-when context `((@context . ,context)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defaults
(*default-graph*
 (or (read-uri (get-environment-variable "MU_DEFAULT_GRAPH"))
     `<http://mu.semte.ch/application/>))

(*sparql-endpoint*
 (or (get-environment-variable "MU_SPARQL_ENDPOINT")
     (if (feature? 'docker)
         "http://database:8890/sparql"
         "http://127.0.0.1:8890/sparql")))

(*sparql-update-endpoint*
 (or (get-environment-variable "MU_SPARQL_UPDATE_ENDPOINT")
     (if (feature? 'docker)
         "http://database:8890/sparql"
         "http://127.0.0.1:8890/sparql")))

(*print-queries?* 
 (let ((val (get-environment-variable "PRINT_SPARQL_QUERIES")))
   (if val (string->boolean val)
       (*print-queries?*))))

(define-namespace mu "http://mu.semte.ch/vocabularies/core/")

(map (lambda (ns) 
       (match-let (((prefix uri)
                    (irregex-split ": " ns)))
         (register-namespace (string->symbol prefix) uri)))
     (string-split (*namespace-definitions*) ","))
)
