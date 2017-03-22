#lang racket/base

(require racket/function file/gunzip json)

(define (process-ct-file file)
  (define-values (in-pipe out-pipe) (make-pipe))
  (call-with-input-file file
    (curryr gunzip-through-ports out-pipe))
  (read-json in-pipe))

(define (read-ct file)
  (display (format "doing ~a~%" file))
  (let* ((json (process-ct-file file))
	 (records (hash-ref json 'Records)))
    (for ([line records])
      (display (format "~a~%" (hash-ref line 'userIdentity))))))

(for ([f (in-directory ".")] #:when (regexp-match? "\\.json.gz$" f))
  (read-ct f))
