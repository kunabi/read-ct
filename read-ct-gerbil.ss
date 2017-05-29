package: read-ct
(export main)

(import
  :gerbil/gambit
  :std/iter
  :std/db/lmdb
  :std/text/json
  :std/format
  :std/text/zlib
  :std/srfi/19)

(def env (lmdb-open (format "~a/metis.db" (user-info-home (user-info (user-name)))) mapsize: 100000000000))
(def db (lmdb-open-db env "metis"))

(def (main . args)
  (let ((count 0)
	(txn (lmdb-txn-begin env)))
    (map
      (lambda (x)
	(read-ct-file x txn)
	(set! count (+ count 1))
	(if (> count 1000)
	  (begin
	    (displayln "GC time!")
	    (lmdb-txn-commit txn)
	    (set! txn (lmdb-txn-begin env))
	    (set! count 0)
	    (gc-report-set! #t)
	    (##gc)
	    (gc-report-set! #f))))
    (list-of-ct-files '("~/CT")))))

(def (read-ct-file file txn)
  (call-with-input-file file
    (lambda (file-input)
      (let ((mytables (hash-ref (read-json (open-input-string (bytes->string (uncompress file-input)))) 'Records)))
	(map
	  (lambda (row)
	    (process-row row txn))
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

(def (process-row row txn)
  (let ((ver (hash-ref row 'eventVersion)))
    (cond
     ((string=? ver "1.01") (parse-row 101-fields row txn))
     ((string=? ver "1.02") (parse-row 102-fields row txn))
     ((string=? ver "1.03") (parse-row 103-fields row txn))
     ((string=? ver "1.04") (parse-row 104-fields row txn))
     ((string=? ver "1.05") (parse-row 105-fields row txn))
     (else (displayln "unsupported version ver:" ver)))))

(def (getf field row)
  (hash-get row field))

(def (parse-row fields row txn)
  ;;(displayln "db?:" (lmdb-db? db) " env?:" (lmdb-env? env) " txn?:" (lmdb-txn? txn))
  (let* ((h (make-hash-table-eq)); 'size: 14))
	 (aws-region (hash-get row 'awsRegion))
	 (event-id (hash-get row 'eventID))
	 (event-name (hash-get row 'eventName))
	 (event-source (hash-get row 'eventSource))
	 (event-time (hash-get row 'eventTime))
	 (event-type (hash-get row 'eventType))
	 (recipient-account-id (hash-get row 'recipientAccountId))
	 (request-id (hash-get row 'requestID))
	 (request-parameters (hash-get row 'requestParameters))
	 (response-elements (hash-get row 'responseElements))
	 (source-ip-address (hash-get row 'sourceIPAddress))
	 (user-agent (hash-get row 'userAgent))
	 (user-identity (hash-get row 'userIdentity))
	 (key (format "~a|~a|~a" event-time event-name event-source)))
    (table-set! h 'aws-region aws-region)
    (table-set! h 'event-id event-id)
    (table-set! h 'event-name event-name)
    (table-set! h 'event-source event-source)
    (table-set! h 'event-time event-time)
    (table-set! h 'event-type event-type)
    (table-set! h 'recipient-account-id recipient-account-id)
    (table-set! h 'request-id request-id)
    (table-set! h 'request-parameters request-parameters)
    (table-set! h 'response-elements response-elements)
    (table-set! h 'source-ip-address source-ip-address)
    (table-set! h 'user-agent user-agent)
    (table-set! h 'user-identity user-identity)
    (lmdb-put txn db key (object->u8vector h))
    ))

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
