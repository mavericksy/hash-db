(in-package #:hash-db-tests)

(def-suite hash-db-suite :description "hash-db tests")
(in-suite hash-db-suite)

;; Each test runs against its own empty set of global tables.
(def-fixture clean-db ()
  (let ((hash-db::*global-tables* (hash-db::create-hash-table)))
    (&body)))

(defun make-people ()
  (make-table "people" '((:id primary-key-column)
                         (:name string)
                         (:age number)
                         (:active boolean))))

(defun make-pets ()
  (make-table "pets" '((:id primary-key-column)
                       (:owner foreign-key-column nil "people")
                       (:pet string))))

(defun add-people ()
  (list (insert-row '(:name "ann" :age 30 :active t) "people")
        (insert-row '(:name "bob" :age 25 :active nil) "people")
        (insert-row '(:name "cat" :age 30 :active t) "people")))

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
  (signals error (make-people)))

(def-test drop-table-removes-it (:fixture clean-db)
  (make-people)
  (is-true (drop-table "people"))
  (is (null (show-tables)))
  (signals error (insert-row '(:name "x") "people")))

(def-test unknown-column-type-is-an-error (:fixture clean-db)
  (signals error (make-table "t" '((:a no-such-type)))))

(def-test duplicate-column-names-are-an-error (:fixture clean-db)
  (signals error (make-table "t" '((:a string) (:a number)))))

(def-test two-primary-keys-are-an-error (:fixture clean-db)
  (signals error (make-table "t" '((:a primary-key-column) (:b primary-key-column)))))

(def-test foreign-key-needs-a-reference-table (:fixture clean-db)
  (signals error (make-table "t" '((:a foreign-key-column)))))

(def-test prebuilt-schema-with-subclass-columns (:fixture clean-db)
  (let ((schema (make-schema '((:id primary-key-column) (:n number)))))
    (finishes (make-table "t" schema))
    (is (eq schema (schema (find-table "t"))))))

(def-test add-column-backfills-default (:fixture clean-db)
  (make-people)
  (add-people)
  (add-column (find-table "people") '(:city string "paris"))
  (is (equal "paris" (getf (get-row 1 "people") :city)))
  (insert-row '(:name "dan" :age 1 :active nil) "people")
  (is (equal "paris" (getf (get-row 4 "people") :city)))
  (signals error (add-column (find-table "people") '(:city string))))

;;; insert, get, update, delete

(def-test insert-returns-ids-and-stores-the-pk (:fixture clean-db)
  (make-people)
  (is (equal '(1 2 3) (add-people)))
  (is (equal '(:id 2 :name "bob" :age 25 :active nil) (get-row 2 "people"))))

(def-test insert-uses-column-defaults (:fixture clean-db)
  (make-table "t" '((:id primary-key-column) (:n number 7)))
  (insert-row '() "t")
  (is (= 7 (getf (get-row 1 "t") :n))))

(def-test false-boolean-is-stored (:fixture clean-db)
  (make-table "t" '((:flag boolean t)))
  (insert-row '(:flag nil) "t")
  (is (eq nil (getf (get-row 1 "t") :flag)))
  (is (eq t (getf (progn (insert-row '() "t") (get-row 2 "t")) :flag))))

(def-test insert-rejects-bad-input (:fixture clean-db)
  (make-people)
  (signals error (insert-row '(:name "x") "no-such-table"))
  (signals error (insert-row '(:nope 1) "people"))
  (signals error (insert-row '(:name) "people"))
  (signals error (insert-row '(:id 99 :name "x") "people")))

(def-test failed-insert-does-not-use-an-id (:fixture clean-db)
  (make-people)
  (ignore-errors (insert-row '(:nope 1) "people"))
  (is (= 1 (insert-row '(:name "ann") "people"))))

(def-test null-in-interned-column-is-an-error (:fixture clean-db)
  (make-table "t" '((:tag interned-string)))
  (signals error (insert-row '() "t")))

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
  (signals error (update-row 99 '(:age 1) "people"))
  (signals error (update-row 1 '(:nope 1) "people"))
  (signals error (update-row 1 '(:id 5) "people")))

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
  (signals error (insert-row '(:owner 99 :pet "rex") "pets"))
  (signals error (insert-row '(:pet "rex") "pets")))

(def-test foreign-key-to-a-missing-table-is-rejected (:fixture clean-db)
  (make-pets)
  (signals error (insert-row '(:owner 1 :pet "rex") "pets")))

(def-test foreign-key-is-checked-on-update (:fixture clean-db)
  (make-people)
  (make-pets)
  (add-people)
  (insert-row '(:owner 1 :pet "rex") "pets")
  (finishes (update-row 1 '(:owner 2) "pets"))
  (signals error (update-row 1 '(:owner 99) "pets")))

;;; select

(def-test select-all-rows (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((result (select :from "people")))
    (is (typep result 'table))
    (is (= 3 (length (row-list result))))
    (is (= 3 (hash-table-count (rows result))))))

(def-test select-result-is-a-copy (:fixture clean-db)
  (make-people)
  (add-people)
  (let ((result (select :from "people")))
    (remhash 1 (rows result))
    (is (= 3 (length (row-list "people"))))))

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
  (signals error (select :from "people" :col :nope))
  (signals error (select :from "people" :order-by :nope))
  (signals error (select :from "no-such-table")))

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
                       (:owner foreign-key-column nil "people")
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
  (signals error (select :from "pets" :inner-join '("people" :owner :id))))

;;; persistence

(defun call-with-temp-file (fn)
  (let ((path (merge-pathnames (format nil "hash-db-test-~A.db" (random 1000000000))
                               (uiop:temporary-directory))))
    (unwind-protect (funcall fn path)
      (when (probe-file path) (delete-file path)))))

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
      (clrhash hash-db::*global-tables*)
      (is (null (show-tables)))
      (load-db path)))
  (is (equal '("people" "pets" "tags") (sort (show-tables) #'string<)))
  (is (equal '(:id 2 :name "bob" :age 25 :active nil) (get-row 2 "people")))
  (is (equal '(:id 1 :owner 1 :pet "rex") (get-row 1 "pets")))
  ;; the id counter continues, constraints and normalisers still work
  (is (= 4 (insert-row '(:name "dan") "people")))
  (signals error (insert-row '(:owner 99 :pet "x") "pets"))
  (signals error (insert-row '() "tags"))
  (is (= 2 (length (row-list (select :from "people" :where (matching "people" :age 30)))))))

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
      (signals error (load-db path))
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
    (is (= 800 (hash-table-count (rows tbl))))
    (is (= 800 (length (remove-duplicates (mapcar (lambda (r) (getf r :id)) (row-list tbl))))))))
