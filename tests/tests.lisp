(in-package #:hash-db-tests)

(def-suite hash-db-suite :description "hash-db tests")
(in-suite hash-db-suite)

;; Each test runs against its own empty database.
(def-fixture clean-db ()
  (let ((old hash-db::*db*))
    (setf hash-db::*db* (hash-db::make-db))
    (unwind-protect (&body)
      (setf hash-db::*db* old))))

;; Remove every table without a file load.
(defun clear-db ()
  (setf (hash-db::db-root hash-db::*db*) (fset:empty-map)))

(defun make-people ()
  (make-table "people" '((:id primary-key-column)
                         (:name string)
                         (:age number)
                         (:active boolean))))

(defun make-pets ()
  (make-table "pets" '((:id primary-key-column)
                       (:owner foreign-key-column :references "people")
                       (:pet string))))

(defun add-people ()
  (list (insert-row '(:name "ann" :age 30 :active t) "people")
        (insert-row '(:name "bob" :age 25 :active nil) "people")
        (insert-row '(:name "cat" :age 30 :active t) "people")))

;;; conditions

(def-test all-errors-share-one-parent (:fixture clean-db)
  (make-people)
  (dolist (thunk (list (lambda () (insert-row '() "no-such-table"))
                       (lambda () (insert-row '(:nope 1) "people"))
                       (lambda () (make-people))))
    (signals hash-db-error (funcall thunk))))

(def-test conditions-carry-data (:fixture clean-db)
  (make-people)
  (handler-case (insert-row '(:nope 1) "people")
    (no-such-column (c) (is (eq :nope (error-column c)))))
  (handler-case (find-table "zzz")
    (no-such-table (c)
      (is (equal "zzz" (error-table c)))
      (is (search "zzz" (princ-to-string c))))))

;;; tables and schema

(def-test column-names-become-keywords (:fixture clean-db)
  (let ((tbl (make-table "t" '((a-number number) ("a-string" string)))))
    (is (equal '(:a-number :a-string) (mapcar #'name (schema tbl))))
    (insert-row '(:a-number 1 :a-string "x") "t")
    (is (equal '(:a-number 1 :a-string "x") (get-row 1 "t")))))

(def-test table-name-can-be-a-string (:fixture clean-db)
  (make-people)
  (is (equal '("people") (show-tables)))
  (is (eq (get-named-table "people") (find-table "people"))))

(def-test duplicate-table-is-an-error (:fixture clean-db)
  (make-people)
  (signals table-exists (make-people)))

(def-test drop-table-removes-it (:fixture clean-db)
  (make-people)
  (is-true (drop-table "people"))
  (is (null (show-tables)))
  (signals no-such-table (insert-row '(:name "x") "people")))

(def-test unknown-column-type-is-an-error (:fixture clean-db)
  (signals schema-error (make-table "t" '((:a no-such-type)))))

(def-test duplicate-column-names-are-an-error (:fixture clean-db)
  (signals schema-error (make-table "t" '((:a string) (:a number)))))

(def-test two-primary-keys-are-an-error (:fixture clean-db)
  (signals schema-error (make-table "t" '((:a primary-key-column) (:b primary-key-column)))))

(def-test foreign-key-needs-a-reference-table (:fixture clean-db)
  (signals schema-error (make-table "t" '((:a foreign-key-column)))))

(def-test prebuilt-schema-with-subclass-columns (:fixture clean-db)
  (let ((schema (make-schema '((:id primary-key-column) (:n number)))))
    (finishes (make-table "t" schema))
    (is (eq schema (schema (find-table "t"))))))

(def-test add-column-backfills-default (:fixture clean-db)
  (make-people)
  (add-people)
  (add-column (find-table "people") '(:city string :default "paris"))
  (is (equal "paris" (getf (get-row 1 "people") :city)))
  (insert-row '(:name "dan" :age 1 :active nil) "people")
  (is (equal "paris" (getf (get-row 4 "people") :city)))
  (signals schema-error (add-column (find-table "people") '(:city string))))

;;; insert, get, update, delete

(def-test insert-returns-ids-and-stores-the-pk (:fixture clean-db)
  (make-people)
  (is (equal '(1 2 3) (add-people)))
  (is (equal '(:id 2 :name "bob" :age 25 :active nil) (get-row 2 "people"))))

(def-test insert-uses-column-defaults (:fixture clean-db)
  (make-table "t" '((:id primary-key-column) (:n number :default 7)))
  (insert-row '() "t")
  (is (= 7 (getf (get-row 1 "t") :n))))

(def-test false-boolean-is-stored (:fixture clean-db)
  (make-table "t" '((:flag boolean :default t)))
  (insert-row '(:flag nil) "t")
  (is (eq nil (getf (get-row 1 "t") :flag)))
  (is (eq t (getf (progn (insert-row '() "t") (get-row 2 "t")) :flag))))

(def-test insert-rejects-bad-input (:fixture clean-db)
  (make-people)
  (signals no-such-table (insert-row '(:name "x") "no-such-table"))
  (signals no-such-column (insert-row '(:nope 1) "people"))
  (signals invalid-row (insert-row '(:name) "people"))
  (signals invalid-row (insert-row '(:id 99 :name "x") "people")))

(def-test failed-insert-does-not-use-an-id (:fixture clean-db)
  (make-people)
  (ignore-errors (insert-row '(:nope 1) "people"))
  (is (= 1 (insert-row '(:name "ann") "people"))))

(def-test null-in-interned-column-is-an-error (:fixture clean-db)
  (make-table "t" '((:tag interned-string)))
  (signals not-null-violation (insert-row '() "t")))

(def-test interned-column-shares-equal-strings (:fixture clean-db)
  (make-table "t" '((:tag interned-string)))
  (insert-row (list :tag (copy-seq "red")) "t")
  (insert-row (list :tag (copy-seq "red")) "t")
  (is (eq (getf (get-row 1 "t") :tag) (getf (get-row 2 "t") :tag))))

(def-test update-changes-only-given-columns (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((old (get-row 2 "people")))
    (update-row 2 '(:age 26) "people")
    (is (equal '(:id 2 :name "bob" :age 26 :active nil) (get-row 2 "people")))
    (is (= 25 (getf old :age)))))

(def-test update-rejects-bad-input (:fixture clean-db)
  (make-people)
  (add-people)
  (signals no-such-row (update-row 99 '(:age 1) "people"))
  (signals no-such-column (update-row 1 '(:nope 1) "people"))
  (signals invalid-row (update-row 1 '(:id 5) "people")))

(def-test delete-removes-a-row (:fixture clean-db)
  (make-people)
  (add-people)
  (is-true (delete-row 2 "people"))
  (is-false (delete-row 2 "people"))
  (is (null (get-row 2 "people")))
  (is (= 4 (insert-row '(:name "dan") "people"))))

;;; foreign keys

(def-test foreign-key-accepts-an-existing-row (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (finishes (insert-row '(:owner 1 :pet "rex") "pets")))

(def-test foreign-key-rejects-a-missing-row (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (signals foreign-key-violation (insert-row '(:owner 99 :pet "rex") "pets")))

(def-test foreign-key-may-be-null (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (insert-row '(:pet "rex") "pets")
  (is (eq :null (getf (get-row 1 "pets") :owner))))

(def-test foreign-key-to-a-missing-table-is-rejected (:fixture clean-db)
  (make-pets)
  (signals foreign-key-violation (insert-row '(:owner 1 :pet "rex") "pets")))

(def-test foreign-key-is-checked-on-update (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (finishes (update-row 1 '(:owner 2) "pets"))
  (signals foreign-key-violation (update-row 1 '(:owner 99) "pets")))

;;; NULL and types

(def-test missing-value-is-null-not-false (:fixture clean-db)
  (make-people)
  (insert-row '(:name "ann") "people")
  (insert-row '(:name "bob" :active nil) "people")
  (is (eq :null (getf (get-row 1 "people") :active)))
  (is (eq nil (getf (get-row 2 "people") :active)))
  (is (eq :null (getf (get-row 1 "people") :age))))

(def-test values-must-match-the-column-type (:fixture clean-db)
  (make-table "t" '((:s string) (:n number) (:i integer) (:b boolean)
                    (:k keyword) (:ts timestamp)))
  (signals type-mismatch (insert-row '(:s 1) "t"))
  (signals type-mismatch (insert-row '(:n "x") "t"))
  (signals type-mismatch (insert-row '(:i 1.5) "t"))
  (signals type-mismatch (insert-row '(:b 1) "t"))
  (signals type-mismatch (insert-row '(:k "x") "t"))
  (signals type-mismatch (insert-row '(:ts -1) "t"))
  (finishes (insert-row (list :s "a" :n 1.5 :i 2 :b nil :k :red :ts (get-universal-time)) "t")))

(def-test type-is-checked-on-update (:fixture clean-db)
  (make-people)
  (add-people)
  (signals type-mismatch (update-row 1 '(:age "old") "people"))
  (is (= 30 (getf (get-row 1 "people") :age))))

(def-test null-is-accepted-by-every-type (:fixture clean-db)
  (make-table "t" '((:s string) (:n number) (:b boolean)))
  (finishes (insert-row '(:s :null :n :null :b :null) "t")))

(def-test float-column-stores-double-floats (:fixture clean-db)
  (make-table "t" '((:f float)))
  (insert-row '(:f 1) "t")
  (insert-row '(:f 1/2) "t")
  (is (eql 1d0 (getf (get-row 1 "t") :f)))
  (is (eql 0.5d0 (getf (get-row 2 "t") :f))))

(def-test any-column-takes-any-object (:fixture clean-db)
  (make-table "t" '((:x any)))
  (insert-row '(:x (1 "two" :three)) "t")
  (is (equal '(1 "two" :three) (getf (get-row 1 "t") :x))))

(def-test bad-default-is-rejected-at-definition (:fixture clean-db)
  (signals type-mismatch (make-table "t" '((:n number :default "x")))))

(def-test bad-column-options-are-rejected (:fixture clean-db)
  (signals schema-error (make-table "t" '((:n number :colour "red"))))
  (signals schema-error (make-table "t" '((:n number :default))))
  (signals schema-error (make-table "t" '((:n number :references "people")))))

(def-test matching-null (:fixture clean-db)
  (make-people)
  (insert-row '(:name "ann" :age 30) "people")
  (insert-row '(:name "bob") "people")
  (is (equal '("bob")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people"
                                       :where (matching "people" :age :null))))))
  (is (equal '("ann")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people"
                                       :where (matching "people" :age 30)))))
      "a NULL value never matches a given value"))

(def-test matching-a-false-boolean-row-value (:fixture clean-db)
  (make-people)
  (add-people)
  (is (= 2 (length (row-list (select :from "people"
                                     :where (matching "people" :active t)))))))

(def-test null-sorts-last (:fixture clean-db)
  (make-people)
  (insert-row '(:name "x") "people")
  (insert-row '(:name "y" :age 5) "people")
  (insert-row '(:name "z" :age 1) "people")
  (is (equal '("z" "y" "x")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people" :order-by :age))))))

(def-test distinct-treats-nulls-as-equal (:fixture clean-db)
  (make-people)
  (insert-row '(:name "a") "people")
  (insert-row '(:name "b") "people")
  (insert-row '(:name "c" :age 1) "people")
  (is (equal '((:age :null) (:age 1))
             (row-list (select :from "people" :col :age :distinct t)))))

(def-test join-never-matches-nulls (:fixture clean-db)
  (make-people)
  (make-table "pets" '((:pet-id primary-key-column)
                       (:owner foreign-key-column :references "people")
                       (:pet string)))
  (add-people)
  (insert-row '(:pet "stray") "pets")
  (insert-row '(:owner 1 :pet "rex") "pets")
  (is (equal '((:pet "rex"))
             (row-list (select :from "pets"
                               :inner-join '("people" :owner :id)
                               :col :pet)))))

(defun call-with-temp-file (fn)
  (let ((path (merge-pathnames (format nil "hash-db-test-~A.db" (random 1000000000))
                               (uiop:temporary-directory))))
    (unwind-protect (funcall fn path)
      (when (probe-file path) (delete-file path)))))

;;; NOT NULL, UNIQUE, CHECK, DEFAULT

(defun positive-p (n) (> n 0))
(defvar *stamp* 0)
(defun next-stamp () (incf *stamp*))

(def-test not-null-option (:fixture clean-db)
  (make-table "t" '((:a string :not-null t) (:b string)))
  (signals not-null-violation (insert-row '(:b "x") "t"))
  (signals not-null-violation (insert-row '(:a :null) "t"))
  (finishes (insert-row '(:a "x") "t"))
  (signals not-null-violation (update-row 1 '(:a :null) "t")))

(def-test not-null-column-can-use-a-default (:fixture clean-db)
  (make-table "t" '((:a string :not-null t :default "none")))
  (insert-row '() "t")
  (is (equal "none" (getf (get-row 1 "t") :a))))

(def-test unique-rejects-a-duplicate-on-insert (:fixture clean-db)
  (make-table "t" '((:email string :unique t) (:n number)))
  (insert-row '(:email "a@x") "t")
  (signals unique-violation (insert-row '(:email "a@x") "t"))
  (finishes (insert-row '(:email "b@x") "t"))
  (is (= 2 (length (row-list "t")))))

(def-test unique-allows-many-nulls (:fixture clean-db)
  (make-table "t" '((:email string :unique t)))
  (insert-row '() "t")
  (insert-row '() "t")
  (insert-row '(:email :null) "t")
  (is (= 3 (length (row-list "t")))))

(def-test unique-on-update (:fixture clean-db)
  (make-table "t" '((:email string :unique t) (:n number)))
  (insert-row '(:email "a") "t")
  (insert-row '(:email "b") "t")
  (signals unique-violation (update-row 2 '(:email "a") "t"))
  (is (equal "b" (getf (get-row 2 "t") :email)))
  (finishes (update-row 1 '(:email "a" :n 1) "t")
            "a row does not clash with itself")
  (finishes (update-row 2 '(:email "c") "t")))

(def-test unique-uses-the-column-comparison (:fixture clean-db)
  (make-table "t" '((:n number :unique t)))
  (insert-row '(:n 1) "t")
  (signals unique-violation (insert-row '(:n 1.0) "t")))

(def-test add-unique-column (:fixture clean-db)
  (make-table "t" '((:a number)))
  (insert-row '(:a 1) "t")
  (finishes (add-column (find-table "t") '(:u number :unique t :default 5)))
  (is (= 5 (getf (get-row 1 "t") :u)))
  (signals unique-violation (insert-row '(:a 2) "t") "the default 5 is already taken")
  (finishes (insert-row '(:a 2 :u 6) "t")))

(def-test add-unique-column-with-a-shared-default-fails (:fixture clean-db)
  (make-table "t" '((:a number)))
  (insert-row '(:a 1) "t")
  (insert-row '(:a 2) "t")
  (signals unique-violation (add-column (find-table "t") '(:u number :unique t :default 5)))
  (is (equal '(:a) (mapcar #'name (schema (find-table "t")))))
  (is (equal '(:a 1) (get-row 1 "t"))))

(def-test add-not-null-column (:fixture clean-db)
  (make-table "t" '((:a number)))
  (finishes (add-column (find-table "t") '(:b number :not-null t)))
  (insert-row '(:a 1 :b 2) "t")
  (signals not-null-violation (add-column (find-table "t") '(:c number :not-null t))))

(def-test check-option (:fixture clean-db)
  (make-table "t" (list '(:n number :check positive-p)
                        (list :m 'number :check (lambda (v) (< v 10)))))
  (finishes (insert-row '(:n 1 :m 2) "t"))
  (signals check-violation (insert-row '(:n 0) "t"))
  (signals check-violation (insert-row '(:m 10) "t"))
  (signals check-violation (update-row 1 '(:n -5) "t"))
  (finishes (insert-row '() "t") "check does not run on NULL"))

(def-test check-runs-after-the-type-check (:fixture clean-db)
  (make-table "t" '((:n number :check positive-p)))
  (signals type-mismatch (insert-row '(:n "x") "t")))

(def-test default-fn-runs-for-each-row (:fixture clean-db)
  (setf *stamp* 0)
  (make-table "t" '((:ts integer :default-fn next-stamp)))
  (insert-row '() "t")
  (insert-row '() "t")
  (insert-row '(:ts 99) "t")
  (is (equal '(1 2 99) (mapcar (lambda (r) (getf r :ts)) (row-list "t")))))

(def-test default-fn-fills-old-rows-on-add-column (:fixture clean-db)
  (setf *stamp* 0)
  (make-table "t" '((:a number)))
  (insert-row '(:a 1) "t")
  (insert-row '(:a 2) "t")
  (add-column (find-table "t") '(:ts integer :default-fn next-stamp))
  (is (equal '(1 2) (sort (mapcar (lambda (r) (getf r :ts)) (row-list "t")) #'<))))

(def-test bad-constraint-options (:fixture clean-db)
  (signals schema-error (make-table "t" '((:n number :default 1 :default-fn next-stamp))))
  (signals schema-error (make-table "t" '((:n number :check 5))))
  (signals schema-error (make-table "t" '((:n number :default-fn "next-stamp")))))

(def-test save-keeps-constraints-given-as-function-names (:fixture clean-db)
  (make-table "t" '((:id primary-key-column)
                    (:n number :check positive-p :unique t :not-null t)
                    (:ts integer :default-fn next-stamp)))
  (insert-row '(:n 1) "t")
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (clear-db)
      (load-db path)))
  (signals check-violation (insert-row '(:n -1) "t"))
  (signals unique-violation (insert-row '(:n 1) "t"))
  (signals not-null-violation (insert-row '() "t"))
  (finishes (insert-row '(:n 2) "t")))

(def-test save-refuses-function-objects (:fixture clean-db)
  (make-table "t" (list (list :n 'number :check (lambda (v) (> v 0)))))
  (call-with-temp-file
    (lambda (path)
      (signals schema-error (save-db path))
      (is (null (probe-file path))))))

;;; referential actions

(defun count-rows (table) (length (row-list table)))

(defun make-owner-and-pets (on-delete)
  (make-people)
  (make-table "pets" (list '(:id primary-key-column)
                           (list :owner 'foreign-key-column :references "people" :on-delete on-delete)
                           '(:pet string)))
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (insert-row '(:owner 1 :pet "tom") "pets")
  (insert-row '(:owner 2 :pet "kit") "pets"))

(def-test delete-is-restricted-by-default (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (signals foreign-key-violation (delete-row 1 "people"))
  (is (= 3 (count-rows "people")))
  (is (= 1 (count-rows "pets")))
  (is-true (delete-row 1 "pets"))
  (is-true (delete-row 1 "people"))
  (is (= 2 (count-rows "people"))))

(def-test delete-cascade (:fixture clean-db)
  (make-owner-and-pets :cascade)
  (is-true (delete-row 1 "people"))
  (is (= 2 (count-rows "people")))
  (is (equal '("kit") (mapcar (lambda (r) (getf r :pet)) (row-list "pets")))))

(def-test delete-cascade-follows-several-levels (:fixture clean-db)
  (make-owner-and-pets :cascade)
  (make-table "toys" (list '(:id primary-key-column)
                           (list :pet 'foreign-key-column :references "pets" :on-delete :cascade)))
  (insert-row '(:pet 1) "toys")
  (insert-row '(:pet 3) "toys")
  (delete-row 1 "people")
  (is (equal '(3) (mapcar (lambda (r) (getf r :pet)) (row-list "toys")))))

(def-test delete-set-null (:fixture clean-db)
  (make-owner-and-pets :set-null)
  (delete-row 1 "people")
  (is (equal '(:null :null 2) (mapcar (lambda (r) (getf r :owner)) (row-list "pets"))))
  (is (= 3 (count-rows "pets"))))

(def-test delete-cascades-through-a-self-reference (:fixture clean-db)
  (make-table "nodes" (list '(:id primary-key-column)
                            (list :parent 'foreign-key-column :references "nodes" :on-delete :cascade)))
  (insert-row '() "nodes")
  (insert-row '(:parent 1) "nodes")
  (insert-row '(:parent 2) "nodes")
  (insert-row '() "nodes")
  (delete-row 1 "nodes")
  (is (equal '(4) (mapcar (lambda (r) (getf r :id)) (row-list "nodes")))))

(def-test blocked-cascade-changes-nothing (:fixture clean-db)
  ;; people -cascade-> pets -restrict-> toys
  (make-owner-and-pets :cascade)
  (make-table "toys" '((:id primary-key-column) (:pet foreign-key-column :references "pets")))
  (insert-row '(:pet 1) "toys")
  (signals foreign-key-violation (delete-row 1 "people"))
  (is (= 3 (count-rows "people")))
  (is (= 3 (count-rows "pets")))
  (is (= 1 (count-rows "toys"))))

(def-test restrict-does-not-block-a-row-that-is-deleted-anyway (:fixture clean-db)
  ;; toys refer to pets (restrict) and to people (cascade). Both go together.
  (make-owner-and-pets :cascade)
  (make-table "toys" (list '(:id primary-key-column)
                           '(:pet foreign-key-column :references "pets")
                           (list :owner 'foreign-key-column :references "people" :on-delete :cascade)))
  (insert-row '(:pet 1 :owner 1) "toys")
  (finishes (delete-row 1 "people"))
  (is (= 0 (count-rows "toys")))
  (is (= 1 (count-rows "pets"))))

(def-test delete-of-a-missing-row-returns-nil (:fixture clean-db)
  (make-people)
  (is-false (delete-row 99 "people")))

(def-test bad-on-delete-options (:fixture clean-db)
  (make-people)
  (signals schema-error
    (make-table "a" '((:o foreign-key-column :references "people" :on-delete :explode))))
  (signals schema-error
    (make-table "b" '((:o foreign-key-column :references "people" :on-delete :set-null :not-null t))))
  (signals schema-error
    (make-table "c" '((:o number :on-delete :cascade)))))

(def-test drop-table-is-blocked-while-referenced (:fixture clean-db)
  (make-people)
  (make-pets)
  (signals schema-error (drop-table "people"))
  (is-true (drop-table "pets"))
  (is-true (drop-table "people")))

(def-test drop-table-allows-a-self-reference (:fixture clean-db)
  (make-table "nodes" '((:id primary-key-column) (:parent foreign-key-column :references "nodes")))
  (is-true (drop-table "nodes")))

(def-test on-delete-survives-save-and-load (:fixture clean-db)
  (make-owner-and-pets :cascade)
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (clear-db)
      (load-db path)))
  (delete-row 1 "people")
  (is (= 1 (count-rows "pets"))))

;;; transactions

(defun names-of (table)
  (mapcar (lambda (r) (getf r :name)) (row-list table)))

(def-test transaction-commits-all-changes (:fixture clean-db)
  (make-people)
  (is (= 2 (with-transaction
             (insert-row '(:name "a") "people")
             (insert-row '(:name "b") "people"))))
  (is (equal '("a" "b") (names-of "people"))))

(def-test transaction-returns-all-values (:fixture clean-db)
  (is (equal '(1 2 3) (multiple-value-list (with-transaction (values 1 2 3))))))

(def-test error-drops-the-whole-transaction (:fixture clean-db)
  (make-people)
  (handler-case (with-transaction
                  (insert-row '(:name "a") "people")
                  (insert-row '(:name "b") "people")
                  (error "boom"))
    (error () nil))
  (is (= 0 (count-rows "people")))
  (is (= 1 (insert-row '(:name "c") "people")) "ids of dropped rows are free again"))

(def-test constraint-failure-drops-earlier-changes (:fixture clean-db)
  (make-table "t" '((:id primary-key-column) (:u string :unique t)))
  (insert-row '(:u "x") "t")
  (signals unique-violation
    (with-transaction
      (insert-row '(:u "y") "t")
      (insert-row '(:u "x") "t")))
  (is (equal '("x") (mapcar (lambda (r) (getf r :u)) (row-list "t")))))

(def-test non-local-exit-drops-the-transaction (:fixture clean-db)
  (make-people)
  (block out
    (with-transaction
      (insert-row '(:name "a") "people")
      (return-from out)))
  (is (= 0 (count-rows "people"))))

(def-test transaction-reads-its-own-writes (:fixture clean-db)
  (make-people)
  (with-transaction
    (insert-row '(:name "a") "people")
    (is (equal '("a") (names-of "people")))
    (update-row 1 '(:name "b") "people")
    (is (equal "b" (getf (get-row 1 "people") :name)))))

(def-test other-threads-see-only-committed-data (:fixture clean-db)
  (make-people)
  (insert-row '(:name "old") "people")
  (flet ((count-in-other-thread ()
           (bt:join-thread (bt:make-thread (lambda () (count-rows "people"))))))
    (with-transaction
      (insert-row '(:name "new") "people")
      (is (= 2 (count-rows "people")))
      (is (= 1 (count-in-other-thread)) "the other thread does not wait and does not see it"))
    (is (= 2 (count-in-other-thread)))))

(def-test nested-transaction-is-a-savepoint (:fixture clean-db)
  (make-people)
  (with-transaction
    (insert-row '(:name "a") "people")
    (handler-case (with-transaction
                    (insert-row '(:name "b") "people")
                    (error "inner"))
      (error () nil))
    (insert-row '(:name "c") "people"))
  (is (equal '("a" "c") (names-of "people"))))

(def-test error-after-a-handled-inner-error-drops-everything (:fixture clean-db)
  (make-people)
  (ignore-errors
    (with-transaction
      (insert-row '(:name "a") "people")
      (handler-case (with-transaction (insert-row '(:name "b") "people") (error "inner"))
        (error () nil))
      (error "outer")))
  (is (= 0 (count-rows "people"))))

(def-test rollback-leaves-the-transaction (:fixture clean-db)
  (make-people)
  (is (null (with-transaction
              (insert-row '(:name "a") "people")
              (rollback)
              (insert-row '(:name "b") "people"))))
  (is (= 0 (count-rows "people"))))

(def-test rollback-only-leaves-the-innermost-transaction (:fixture clean-db)
  (make-people)
  (with-transaction
    (insert-row '(:name "a") "people")
    (with-transaction
      (insert-row '(:name "b") "people")
      (rollback))
    (insert-row '(:name "c") "people"))
  (is (equal '("a" "c") (names-of "people"))))

(def-test rollback-outside-a-transaction-is-an-error (:fixture clean-db)
  (signals transaction-error (rollback)))

(def-test transaction-spans-tables (:fixture clean-db)
  (make-people)
  (make-pets)
  (with-transaction
    (let ((owner (insert-row '(:name "ann") "people")))
      (insert-row (list :owner owner :pet "rex") "pets")))
  (is (= 1 (count-rows "people")))
  (is (= 1 (count-rows "pets")))
  (ignore-errors
    (with-transaction
      (insert-row '(:name "bob") "people")
      (insert-row '(:owner 99 :pet "x") "pets")))
  (is (= 1 (count-rows "people")) "the new owner is dropped with the failed pet"))

(def-test table-changes-roll-back (:fixture clean-db)
  (make-people)
  (ignore-errors
    (with-transaction
      (make-table "extra" '((:a number)))
      (drop-table "people")
      (error "boom")))
  (is (equal '("people") (show-tables)))
  (is (equal '(:id :name :age :active) (mapcar #'name (schema (find-table "people")))))
  (ignore-errors
    (with-transaction
      (add-column (find-table "people") '(:city string))
      (error "boom")))
  (is (= 4 (length (schema (find-table "people"))))))

(def-test load-is-not-allowed-inside-a-transaction (:fixture clean-db)
  (make-people)
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (signals transaction-error (with-transaction (load-db path))))))

(def-test transactions-do-not-lose-updates (:fixture clean-db)
  (make-table "c" '((:id primary-key-column) (:n integer :default 0)))
  (insert-row '() "c")
  (let ((threads (loop repeat 4
                       collect (bt:make-thread
                                 (lambda ()
                                   (loop repeat 50
                                         do (with-transaction
                                              (update-row 1 (list :n (1+ (getf (get-row 1 "c") :n)))
                                                          "c"))))))))
    (mapc #'bt:join-thread threads))
  (is (= 200 (getf (get-row 1 "c") :n))))

;;; snapshots

(def-test snapshot-reads-one-fixed-state (:fixture clean-db)
  (make-people)
  (add-people)
  (with-snapshot
    (insert-row '(:name "dan") "people")
    (delete-row 1 "people")
    (is (= 3 (count-rows "people")) "the snapshot does not see changes made after it started")
    (is (equal '("ann" "bob" "cat") (names-of (select :from "people")))))
  (is (= 3 (count-rows "people")))
  (is (equal '("bob" "cat" "dan") (names-of "people"))))

(def-test snapshot-ignores-commits-of-other-threads (:fixture clean-db)
  (make-people)
  (add-people)
  (with-snapshot
    (bt:join-thread (bt:make-thread (lambda () (insert-row '(:name "dan") "people"))))
    (is (= 3 (count-rows "people")))
    (is (= 3 (length (row-list (select :from "people"))))))
  (is (= 4 (count-rows "people"))))

(def-test snapshot-covers-all-tables-of-a-join (:fixture clean-db)
  (make-people)
  (make-table "pets" '((:pet-id primary-key-column)
                       (:owner foreign-key-column :references "people")
                       (:pet string)))
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (with-snapshot
    (insert-row '(:owner 2 :pet "tom") "pets")
    (is (= 1 (count-rows (select :from "pets" :inner-join '("people" :owner :id)))))))

(def-test snapshot-inside-a-transaction-reads-the-working-state (:fixture clean-db)
  (make-people)
  (with-transaction
    (insert-row '(:name "a") "people")
    (with-snapshot
      (is (= 1 (count-rows "people"))))))

(def-test select-keeps-a-table-stable-after-it-returns (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((result (select :from "people" :order-by :age)))
    (update-row 1 '(:age 99) "people")
    (is (equal '(25 30 30) (mapcar (lambda (r) (getf r :age)) (row-list result))))))

;;; select

(def-test select-all-rows (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((result (select :from "people")))
    (is (typep result 'table))
    (is (= 3 (length (row-list result))))
    (is (= 3 (fset:size (rows result))))))

(def-test select-result-does-not-change-with-the-table (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((result (select :from "people")))
    (delete-row 1 "people")
    (insert-row '(:name "dan") "people")
    (insert-row '(:name "eve") "people")
    (is (= 3 (length (row-list result))))
    (is (= 4 (length (row-list "people"))))))

(def-test select-where-with-matching (:fixture clean-db)
  (make-people)
  (add-people)
  (is (equal '("ann" "cat")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people"
                                       :where (matching "people" :age 30))))))
  (is (equal '("cat")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people"
                                       :where (matching "people" :age 30 :name "cat")))))
      "matching with two columns needs both to match")
  (is (null (row-list (select :from "people"
                              :where (matching "people" :age 30 :name "bob"))))
      "bob is 25, so no row matches"))

(def-test select-where-with-a-function (:fixture clean-db)
  (make-people)
  (add-people)
  (is (= 1 (length (row-list (select :from "people"
                                     :where (lambda (r) (< (getf r :age) 26))))))))

(def-test select-projects-columns (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((result (select :from "people" :col '(:name :age))))
    (is (equal '(:name :age) (mapcar #'name (schema result))))
    (is (equal '(:name "ann" :age 30) (first (row-list result)))))
  (is (equal '(:name "ann") (first (row-list (select :from "people" :col :name))))))

(def-test select-unknown-column-is-an-error (:fixture clean-db)
  (make-people)
  (signals no-such-column (select :from "people" :col :nope))
  (signals no-such-column (select :from "people" :order-by :nope))
  (signals no-such-table (select :from "no-such-table")))

(def-test select-distinct (:fixture clean-db)
  (make-people)
  (add-people)
  (is (equal '((:age 30) (:age 25))
             (row-list (select :from "people" :col :age :distinct t)))))

(def-test select-order-by-one-column (:fixture clean-db)
  (make-people)
  (add-people)
  (is (equal '("bob" "ann" "cat")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people" :order-by :age))))))

(def-test select-order-by-two-columns (:fixture clean-db)
  (make-people)
  (insert-row '(:name "zed" :age 30) "people")
  (insert-row '(:name "amy" :age 30) "people")
  (insert-row '(:name "bob" :age 20) "people")
  (is (equal '("bob" "amy" "zed")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people" :order-by '(:age :name)))))))

(def-test select-order-by-a-column-not-projected (:fixture clean-db)
  (make-people)
  (add-people)
  (is (equal '((:name "bob") (:name "ann") (:name "cat"))
             (row-list (select :from "people" :col :name :order-by :age)))))

(def-test select-from-a-select-result (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((older (select :from "people" :where (matching "people" :age 30))))
    (is (= 2 (length (row-list (select :from older :col :name)))))))

(def-test select-inner-join (:fixture clean-db)
  (make-people)
  (make-table "pets" '((:pet-id primary-key-column)
                       (:owner foreign-key-column :references "people")
                       (:pet string)))
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (insert-row '(:owner 1 :pet "tom") "pets")
  (insert-row '(:owner 3 :pet "kit") "pets")
  (let ((result (select :from "pets"
                        :inner-join '("people" :owner :id)
                        :col '(:pet :name)
                        :order-by :pet)))
    (is (equal '((:pet "kit" :name "cat")
                 (:pet "rex" :name "ann")
                 (:pet "tom" :name "ann"))
               (row-list result)))))

(def-test select-inner-join-with-clashing-names-is-an-error (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (signals query-error (select :from "pets" :inner-join '("people" :owner :id))))

;;; persistence

(def-test save-and-load-round-trip (:fixture clean-db)
  (make-people)
  (make-pets)
  (make-table "tags" '((:tag interned-string)))
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (insert-row '(:tag "red") "tags")
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (clear-db)
      (is (null (show-tables)))
      (load-db path)))
  (is (equal '("people" "pets" "tags") (sort (show-tables) #'string<)))
  (is (equal '(:id 2 :name "bob" :age 25 :active nil) (get-row 2 "people")))
  (is (equal '(:id 1 :owner 1 :pet "rex") (get-row 1 "pets")))
  ;; the id counter continues, constraints and normalisers still work
  (is (= 4 (insert-row '(:name "dan") "people")))
  (signals foreign-key-violation (insert-row '(:owner 99 :pet "x") "pets"))
  (signals not-null-violation (insert-row '() "tags"))
  (is (= 2 (length (row-list (select :from "people" :where (matching "people" :age 30)))))))

(def-test interned-column-still-works-after-load (:fixture clean-db)
  (make-table "tags" '((:tag interned-string :unique t)))
  (insert-row (list :tag (copy-seq "red")) "tags")
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (clear-db)
      (load-db path)))
  (is (= 1 (count-rows (select :from "tags" :where (matching "tags" :tag (copy-seq "red"))))))
  (signals unique-violation (insert-row (list :tag (copy-seq "red")) "tags")))

(def-test load-replaces-existing-tables (:fixture clean-db)
  (make-people)
  (call-with-temp-file
    (lambda (path)
      (save-db path)
      (make-table "extra" '((:a number)))
      (load-db path)))
  (is (equal '("people") (show-tables))))

(def-test load-rejects-a-foreign-file (:fixture clean-db)
  (call-with-temp-file
    (lambda (path)
      (cl-store:store '(1 2 3) path)
      (make-people)
      (signals file-format-error (load-db path))
      (is (equal '("people") (show-tables))))))

;;; threads

(def-test parallel-inserts-get-unique-ids (:fixture clean-db)
  (let* ((tbl (make-table "t" '((:id primary-key-column) (:n number))))
         (threads (loop for i below 4
                        collect (let ((i i))
                                  (bt:make-thread
                                    (lambda ()
                                      (loop for n below 200
                                            do (insert-row (list :n (+ (* i 1000) n)) tbl))))))))
    (mapc #'bt:join-thread threads)
    (is (= 800 (fset:size (rows (find-table "t")))))
    (is (= 800 (length (remove-duplicates (mapcar (lambda (r) (getf r :id)) (row-list "t"))))))))
