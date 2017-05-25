package: read-ct
(export main)
(import :std/iter)

(def (main . args)
  (list-of-ct-files "~/cs/"))

(def (find-files file-or-dir filter)
  (displayln "find-files: " file-or-dir)
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
