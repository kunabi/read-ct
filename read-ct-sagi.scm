#!/usr/bin/env sagittarius

(import (rnrs)
	(rfc gzip)
	(json)
	(util file)
	(pp)
	(srfi :13)
	(srfi :115 regexp)
	(srfi :26 cut))

(define (get-type x)
  (cond ((number? x) "Number")
	((pair? x) "Pair")
	((vector? x) "Vector")
	((hashtable? x) "Hashtable")
	((string? x) "String")
	((#t) "Unknown type")))


(define (read-ct file)									   ;;
  (let* ((in (open-file-input-port file))							   ;;
	 (mygz (open-gzip-input-port in))							   ;;
	 (myvec (get-bytevector-all mygz))							   ;;
	 (mystr (open-string-input-port (bytevector->string myvec (native-transcoder))))	   ;;
	 (myjson (json-read mystr))								   ;;
	 (entries (cdr (car (vector->list myjson)))))						   ;;
    (for-each (lambda (x)									   ;;
		(format #t "type:~s~%"  (assoc "userIdentity" (vector->list x))))		   ;;
	      entries)										   ;;
    (close-port mystr)									   ;;
    (close-port mygz)									   ;;
    (close-port in)										   ;;
  ))												   ;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (define (read-ct file)																						     ;;
;;   (let																								     ;;
;;       ((entries (cdr (car (vector->list (json-read (open-string-input-port (bytevector->string (get-bytevector-all (open-gzip-input-port (open-file-input-port file))) (native-transcoder)))))))))	     ;;
;;     (for-each (lambda (x)																						     ;;
;; 		(format #t "type:~s~%"  (assoc "userIdentity" (vector->list x))))															     ;;
;; 	      entries)))																						     ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(path-for-each "."
	       (lambda (p t)
		 (cond ((string-suffix? "json.gz" p)
			(read-ct p))))
	       :file-only #t
	       :recursive #t)
