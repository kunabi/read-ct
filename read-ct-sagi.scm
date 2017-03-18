#!/usr/bin/env sagittarius

(import (rnrs)
	(rfc gzip)
	(json)
	(pp)
	(srfi :26 cut))

(define (get-type x)
  (cond ((number? x) "Number")
	((pair? x) "Pair")
	((vector? x) "Vector")
	((hashtable? x) "Hashtable")
	((string? x) "String")
	((#t) "Unknown type")))

(define myfile (open-file-input-port "/home/akkad/test.json.gz"))
(define mygz (open-gzip-input-port myfile))
(define myvec (get-bytevector-all mygz))
(define mystr (open-string-input-port (bytevector->string myvec (native-transcoder))))
(define myjson (json-read mystr))
(define entries (cdr (car (vector->list myjson))))

(for-each (lambda (x)
	    (format #t "type:~A~%"  (vector->list x))
	    ;;(format #t "type:~A~%" (assoc 'userIdentity (vector->list x)))
	  )
	  entries)
