package: read-ct
(export main)
(import :std/iter :std/text/zlib :std/text/json)


(def (main . args)
    (for ((x (list-of-ct-files '("~/cs/"))))
	 ;;(read-ct-file x))
	 (displayln x)))

(def (read-ct-file file)
  (displayln "reading:" file)
  (map
    (lambda (line)
      (displayln "line:" (hash->list line)))
    (call-with-input-file file
      (lambda (file-input)
	(hash->list (read-json (open-input-string (bytes->string (uncompress file-input)))))))))


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
                    (and (equal? (path-extension filename) ".json.gz")
                         (not (equal? (path-strip-directory filename) "#.json.gz"))))))
    args)))
