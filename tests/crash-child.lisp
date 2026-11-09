;;;; Child process of the crash-recovery test. It is not part of the test suite
;;;; system. The test starts it, reads the ids it prints, and kills it.
;;;;
;;;; The parent defines CL-USER::*DB-PATH* and CL-USER::*CHECKPOINT-BYTES* first.
;;;; The child opens the database, then adds rows to tables "a" and "b" in one
;;;; transaction each. It prints the number of each transaction after the commit
;;;; returns, so every printed number is a commit the database promised to keep.

(push (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system :hash-db)

(in-package :hash-db)

(open-db cl-user::*db-path* :checkpoint-bytes cl-user::*checkpoint-bytes*)

(unless (get-named-table "a")
  (dolist (name '("a" "b"))
    (make-table name '((:id primary-key-column) (:n integer) (:s string)))))

(loop for i from (1+ (length (row-list "a")))
      do (with-transaction
           (dolist (name '("a" "b"))
             (insert-row (list :n i :s (format nil "row ~D" i)) name)))
         (format t "~D~%" i)
         (finish-output))
