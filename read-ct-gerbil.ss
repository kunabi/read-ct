package: read-ct
(export main)

(import
  :gerbil/gambit
  :std/iter
  :std/text/json
  :std/text/zlib
  :std/srfi/19)

(def (main . args)
  (gc-report-set! #t)
  (map
    (lambda (x)
      (read-ct-file x))
      (list-of-ct-files '("~/CT"))))

(def (read-ct-file file)
     (call-with-input-file file
       (lambda (file-input)
	 (let (( mytables (hash-ref (read-json (open-input-string (bytes->string (uncompress file-input)))) 'Records)))
	   (map
	    (lambda (row)
	      (process-row row))
	      ;;(displayln (hash->list (hash-ref row 'userIdentity))))
	    mytables)))))

(define 101-fields (list
		    'awsRegion
		    'eventID
		    'eventName
		    'eventSource
		    'eventTime
		    'eventType
		    'recipientAccountId
		    'requestID
		    'requestParameters
		    'responseElements
		    'sourceIPAddress
		    'userAgent
		    'userIdentity
		    ))

(define 102-fields 101-fields)
(define 103-fields 101-fields)
(define 104-fields 101-fields)
(define 105-fields 101-fields)

(def (process-row row)
  (let ((ver (hash-ref row 'eventVersion)))
    (cond
     ((string=? ver "1.01") (parse-row 101-fields row))
     ((string=? ver "1.02") (parse-row 102-fields row))
     ((string=? ver "1.03") (parse-row 103-fields row))
     ((string=? ver "1.04") (parse-row 104-fields row))
     ((string=? ver "1.05") (parse-row 105-fields row))
     (else (displayln "unsupported version ver:" ver)))))

(def (parse-row fields row)
  (map
    (lambda (field)
      (let ((val (hash-get row field)))
	(displayln "field:" field " value:" val)))
    fields))

(def (find-files file-or-dir filter)
  (if (eq? (file-type file-or-dir) 'directory)
      (apply
       append
       (map
        (lambda (f)
          (find-files (path-expand f file-or-dir) filter))
        (directory-files file-or-dir)))
      (if (filter file-or-dir)
          (list file-or-dir)
          (list))))

(def (list-of-ct-files args)
  (apply
   append
   (map
    (lambda (f)
      (find-files f
                  (lambda (filename)
                    (and (equal? (path-extension filename) ".gz")
                         (not (equal? (path-strip-directory filename) "#.gz"))))))
    args)))
