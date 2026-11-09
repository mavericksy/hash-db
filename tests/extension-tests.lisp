(in-package #:hash-db-tests)

(in-suite hash-db-suite)

;;; A toy feature that uses the extension points: named notes in the root.
;;; A note is the list (:note text). Notes share the namespace with tables.

(defun note (name)
  (let ((object (values (fset:lookup (hash-db::current-root) name))))
    (and (consp object) (eq (first object) :note) (second object))))

(defun put-note (name text)
  (with-transaction
    (setf (hash-db::txn-root hash-db::*txn*)
          (fset:with (hash-db::txn-root hash-db::*txn*) name (list :note text)))
    (hash-db::journal-add (list :test-note name text))
    text))

(hash-db::define-replay :test-note (root name text)
  (fset:with root name (list :note text)))

(hash-db::register-snapshot-section
 :test-notes
 (lambda (root)
   (loop for name in (fset:convert 'list (fset:domain root))
         for object = (values (fset:lookup root name))
         when (and (consp object) (eq (first object) :note))
           collect (list name (second object))))
 (lambda (root data)
   (loop for (name text) in data
         do (setf root (fset:with root name (list :note text))))
   root))

(hash-db::register-save-check
 (lambda (root)
   (fset:do-map (name object root)
     (when (and (consp object) (eq (first object) :note) (equal (second object) "unsavable"))
       (error 'persistence-error :message (format nil "Note ~A can't be saved" name))))))

(def-test other-things-in-the-root-are-not-tables (:fixture clean-db)
  (make-people)
  (put-note "memo" "hello")
  (is (equal '("people") (show-tables)))
  (is (equal "hello" (note "memo")))
  (signals no-such-table (find-table "memo"))
  (signals no-such-table (insert-row '(:name "x") "memo"))
  (is-false (drop-table "memo") "drop-table only drops tables")
  (signals table-exists (make-table "memo" '((:a number))))
  (is (indexes-match-rows-p)))

(def-test extension-entries-replay-from-the-log (:fixture clean-db)
  (with-temp-dir (dir)
    (let ((path (merge-pathnames "t.db" dir)))
      (open-db path)
      (make-people)
      (put-note "memo" "from the log")
      (restart-db path)
      (is (equal "from the log" (note "memo")))
      (is (equal '("people") (show-tables))))))

(def-test extension-sections-survive-snapshots (:fixture clean-db)
  (make-people)
  (put-note "memo" "in a snapshot")
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (clear-db)
      (is (null (note "memo")))
      (load-db path)))
  (is (equal "in a snapshot" (note "memo")))
  (with-temp-dir (dir)
    (let ((path (merge-pathnames "t.db" dir)))
      (open-db path)
      (put-note "other" "checkpointed")
      (checkpoint)
      (restart-db path)
      (is (equal "checkpointed" (note "other"))))))

(def-test save-checks-can-refuse-a-save (:fixture clean-db)
  (put-note "bad" "unsavable")
  (call-with-temp-file
    (lambda (path)
      (signals persistence-error (save-db path))
      (is (null (probe-file path))))))

(def-test unknown-log-entries-are-an-error (:fixture clean-db)
  (signals persistence-error (hash-db::replay-entries (fset:empty-map) '((:no-such-kind 1)))))

(def-test version-4-files-still-load (:fixture clean-db)
  (call-with-temp-file
    (lambda (path)
      (cl-store:store (list :hash-db 4 0
                            (list (list "t" 1 '((:id primary-key-column) (:e string)) '((1 :id 1 :e "a"))
                                        '((:by-e :e nil)))))
                      path)
      (load-db path)))
  (is (equal '(:by-e) (index-names "t"))))
