(ql:quickload '(:jonathan :gzip-stream :cl-fad :fare-memoization :trivial-dump-core))


(defvar *fields* '(
		   ;; :additionalEventData
		   ;; :awsRegion
		   ;; :errorCode
		   ;; :errorMessage
		   ;; :eventID
		   ;; :eventName
		   ;; :eventSource
		   ;; :eventTime
		   ;; :eventType
		   ;; :eventVersion
		   ;; :recipientAccountId
		   ;; :requestID
		   ;; :requestParameters
		   ;; :resources
		   ;; :responseElements
		   ;; :sourceIPAddress
		   ;; :userAgent
		   :userIdentity
		   ;;:userName
		   ))


(defun error-print (fn error)
  (format t "~%~A~%~A~%~A~%~A~%~A~%" fn fn error fn fn))

(defun read-json-gzip-file (file)
  (handler-case
      (progn
	(let ((json (get-json-gzip-contents file)))
	  (jonathan:parse json)))
    (t (e) (error-print "read-json-gzip-file" e))))

(defun get-json-gzip-contents (file)
  (handler-case
      (progn (first (gzip-stream:with-open-gzip-file (in file)
		      (loop for l = (read-line in nil nil)
			 while l collect l))))
    (t (e) (error-print "get-json-gzip-contents" e))))

(defun process-ct-file (x)
  "Handle the contents of the json gzip file"
  (when (equal (pathname-type x) "gz")
    (parse-ct-contents x)))

(defun parse-ct-contents (x)
  "process the json output"
  (handler-case
      (progn
	(let* ((records (second (read-json-gzip-file x)))
	       (num (length records))
	       (btime (get-internal-real-time)))
	  (dolist (x records)
	    (format t "~A~%" (process-record x *fields*)))
	    ;;(normalize-insert
	  (let* ((etime (get-internal-real-time))
		 (delta (/ (float (- etime btime)) (float internal-time-units-per-second)))
		 (rps (ignore-errors (/ (float num) (float delta)))))
	    (if (> num 100)
		(format t "~%rps:~A rows:~A delta:~A" rps num delta))
	    )))
    (t (e) (error-print "read-json-gzip-file" e)))
  ;;#+sbcl (room) ;; (trivial-garbage:gc)
  )

(defun process-record (record fields)
  (loop for i in fields
     collect (make-safe-string (get-value i record))))

(defun walk-ct (path fn)
  (cl-fad:walk-directory path fn))

(defun read-ct ()
    (walk-ct "."
	     #'process-ct-file))

(defun fetch-value (indicators plist)
  "Return the value at the end of the indicators list"
  (reduce #'getf indicators :initial-value plist))

(defun get-value (field record)
  (cond
    ((equal :userIdentity field)(getf record :|userIdentity|))
    (t (format nil "Unknown arg:~A~%" field))))

(fare-memoization:define-memo-function get-hostname-by-ip (ip)
  (if (boundp '*benching*)
      "bogus.example.com"
      (progn
	(if (cl-ppcre:all-matches "^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$" ip)
	    (let ((name
		   #+allegro
		    (ignore-errors (socket:ipaddr-to-hostname ip))
		    #+sbcl
		    (ignore-errors (sb-bsd-sockets:host-ent-name
				    (sb-bsd-sockets:get-host-by-address
				     (sb-bsd-sockets:make-inet-address ip))))
		    #+lispworks
		    (ignore-errors (comm:get-host-entry ip :fields '(:name)))
		    #+clozure
		    (ignore-errors (ccl:ipaddr-to-hostname (ccl:dotted-to-ipaddr ip)))))
	      (if (null name)
		  ip
		  name))
	    ip))))

(defun make-safe-string (str)
  (if (stringp str)
      (replace-all str "'" "")
      str))


#+sbcl (sb-ext:save-lisp-and-die "read-ct-sbcl"  :executable t :toplevel 'read-ct :save-runtime-options t)
#-sbcl (trivial-dump-core:save-executable "read-ct-cl" #'read-ct)
