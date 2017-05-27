package: read-ct
(export main)
(import :std/iter :std/text/zlib :std/text/json :std/srfi/19)

(def (main . args)
  (map
    (lambda (x)
      (read-ct-file x))
      (list-of-ct-files '("."))))

(def (read-ct-file file)
     (displayln "doing:" file)
     (call-with-input-file file
       (lambda (file-input)
	 (let (( mytables (hash-ref (read-json (open-input-string (bytes->string (uncompress file-input)))) 'Records)))
	   (map
	    (lambda (row)
	      (displayln (hash->list (hash-ref row 'userIdentity))))
	    mytables)))))

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
