(use z3)
(use medea)
(use vector-lib)
(use posix)
(use files)
(use srfi-13)

(define (parse-json-gz-file file)
  (let* ((gzip-stream (z3:open-compressed-input-file file))
	 (json (read-json gzip-stream)))
    (close-input-port gzip-stream)
    json))

(define (read-ct file)
  (format #t "doing file:~A~%" file)
  (let* ((json (parse-json-gz-file file))
	 (entries (vector->list (cdr (car json)))))
    (for-each
     (lambda (x)
       (format #t "~A~%" (assoc 'userIdentity x)))
     entries)))

(define (walk FN PATH)
  (for-each (lambda (ENTRY)
	      (cond ((not (null? ENTRY))
		     (let ((MYPATH (make-pathname PATH ENTRY)))
		       (cond ((directory-exists? MYPATH)
			      (walk FN MYPATH)))
		       (FN MYPATH) )))) (directory PATH #t) ))


(walk
 (lambda (X)
   (cond ((string-suffix? ".json.gz" X)
	  (read-ct X))))
 ".")
