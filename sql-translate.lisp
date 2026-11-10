;;;; SQL: from the tree to the expressions of hash-db.
;;;;
;;;; A scope says which tables a query can name. It has one entry for each table of
;;;; FROM and JOIN: the name a query uses for it (its alias, or the table name), the
;;;; table, its columns, and the prefix that its columns carry in the rows. The first
;;;; table has no prefix, so its columns are :NAME. A joined table has its qualifier as
;;;; prefix, so its columns are :PETS.NAME, as an :as prefix on a join gives.

(in-package :hash-db)

(defstruct (scope-entry (:constructor make-scope-entry (qualifier table schema prefix)))
  qualifier
  table
  schema
  prefix)

(defvar *scope* nil)

;; The values of the ? parameters, as a vector.
(defvar *params* #())

;; Names of select items. HAVING and ORDER BY can use them, as keywords.
(defvar *aliases* nil)

(defun scope-for-table (table-name table)
  (list (make-scope-entry table-name table (schema table) nil)))

(defun column-keyword (entry column)
  (let ((name (name column)))
    (if (scope-entry-prefix entry)
        (prefixed-name (scope-entry-prefix entry) name)
        name)))

;; The column of SCHEMA that a name in SQL means. SQL names have no hyphen, so pet_ref also
;; finds the column PET-REF. An exact match comes first.
(defun sql-find-column (name schema)
  (or (find (as-keyword name) schema :key #'name)
      (and (find #\_ name)
           (find (as-keyword (substitute #\- #\_ name)) schema :key #'name))))

;; Look for a column in SCOPE. Return (values entry column), or nil and :none or :ambiguous.
(defun find-scope-column (scope qualifier name)
  (progn
    (if qualifier
        (let* ((entry (find qualifier scope :key #'scope-entry-qualifier :test #'string-equal))
               (column (and entry (sql-find-column name (scope-entry-schema entry)))))
          (if column (values entry column) (values nil :none)))
        (let ((matches (loop for entry in scope
                             for column = (sql-find-column name (scope-entry-schema entry))
                             when column collect (cons entry column))))
          (cond ((null matches) (values nil :none))
                ((rest matches) (values nil :ambiguous))
                (t (values (car (first matches)) (cdr (first matches)))))))))

;; The entry and the column that a name means in the scope. Signal sql-error if there is none.
(defun resolve-column-entry (qualifier name)
  (multiple-value-bind (entry column) (find-scope-column *scope* qualifier name)
    (cond (entry (values entry column))
          ((and qualifier (not (find qualifier *scope* :key #'scope-entry-qualifier :test #'string-equal)))
           (sql-fail *sql-text* nil "Unknown table or alias ~A" qualifier))
          (qualifier (sql-fail *sql-text* nil "Table ~A has no column ~A" qualifier name))
          ((eq column :ambiguous)
           (sql-fail *sql-text* nil "Column ~A is in more than one table. Write table.~A" name name))
          (t (sql-fail *sql-text* nil "Unknown column ~A" name)))))

;; The keyword that names a column in the rows, for a (:col qualifier name) in the scope.
(defun resolve-column (qualifier name)
  (if (and (null qualifier) (find (as-keyword name) *aliases*)
           (not (find-scope-column *scope* nil name)))
      (as-keyword name)
      (multiple-value-bind (entry column) (resolve-column-entry qualifier name)
        (column-keyword entry column))))

;; A value from a parameter, as data an expression accepts as a constant.
(defun param-datum (value)
  (cond ((or (stringp value) (numberp value) (eq value t) (null value) (eq value :null)) value)
        (t (list :quote value))))

(defun sql-param (index)
  (unless (< index (length *params*))
    (sql-fail *sql-text* nil "The statement has more ? than the ~D value~:P given" (length *params*)))
  (param-datum (aref *params* index)))

(defparameter *sql-binary-ops*
  '(("=" . "=") ("<>" . "/=") ("<" . "<") (">" . ">") ("<=" . "<=") (">=" . ">=")
    ("+" . "+") ("-" . "-") ("*" . "*") ("/" . "/") ("||" . "CONCAT")))

(defun dsl-op (name)
  (make-keyword name))

;; Fold nested AND, or nested OR, into one form.
(defun join-logic (op a b)
  (flet ((parts (x) (if (and (consp x) (eq (first x) (dsl-op op))) (rest x) (list x))))
    (cons (dsl-op op) (append (parts a) (parts b)))))

(defparameter *sql-functions*
  '("LOWER" "UPPER" "LENGTH" "CHAR_LENGTH" "COALESCE" "COUNT" "SUM" "AVG" "MIN" "MAX" "JSON_GET"
    "NOW" "NEXTVAL" "CURRVAL"))

(defun check-function-arity (name args min &optional (max min))
  (let ((n (length args)))
    (when (or (< n min) (and max (> n max)))
      (sql-fail *sql-text* nil "~A takes ~A argument~:P, got ~D" name
                (cond ((null max) (format nil "~D or more" min))
                      ((= min max) min)
                      (t (format nil "~D to ~D" min max)))
                n))))

(defun translate-call (name args distinct star)
  (let ((args (mapcar #'translate-expression args)))
    (cond
      ((string= name "COUNT")
       (cond (star (list (dsl-op "COUNT")))
             (distinct (check-function-arity name args 1) (list (dsl-op "COUNT-DISTINCT") (first args)))
             (t (check-function-arity name args 1) (list (dsl-op "COUNT") (first args)))))
      (star (sql-fail *sql-text* nil "~A(*) is not supported" name))
      ((member name '("LOWER" "UPPER" "LENGTH") :test #'string=)
       (check-function-arity name args 1)
       (list (dsl-op name) (first args)))
      ((string= name "CHAR_LENGTH")
       (check-function-arity name args 1)
       (list (dsl-op "LENGTH") (first args)))
      ((string= name "COALESCE")
       (check-function-arity name args 1 nil)
       (cons (dsl-op "COALESCE") args))
      ((member name '("SUM" "AVG" "MIN" "MAX") :test #'string=)
       (check-function-arity name args 1)
       (list (dsl-op name) (first args)))
      ((string= name "JSON_GET")
       (check-function-arity name args 1 nil)
       (cons (dsl-op "JSON-GET") args))
      ((string= name "NOW")
       (check-function-arity name args 0)
       (list (dsl-op "FN") 'now))
      ((string= name "NEXTVAL")
       (check-function-arity name args 1)
       (list (dsl-op "FN") 'next-value (first args)))
      ((string= name "CURRVAL")
       (check-function-arity name args 1)
       (list (dsl-op "FN") 'current-value (first args)))
      (t (sql-fail *sql-text* nil "The function ~A is not supported. Supported: ~{~A~^, ~}"
                   name *sql-functions*)))))

;; The expression of hash-db for a tree from the parser.
(defun translate-expression (ast)
  (ecase (first ast)
    (:lit (second ast))
    (:param (sql-param (second ast)))
    (:col (resolve-column (second ast) (third ast)))
    (:op
     (destructuring-bind (name &rest args) (rest ast)
       (cond ((member name '("AND" "OR") :test #'string=)
              (join-logic name (translate-expression (first args)) (translate-expression (second args))))
             ((string= name "NOT") (list (dsl-op "NOT") (translate-expression (first args))))
             ((string= name "NEG") (list (dsl-op "-") (translate-expression (first args))))
             ((string= name "%") (sql-fail *sql-text* nil "The % operator is not supported"))
             (t (list (dsl-op (cdr (assoc name *sql-binary-ops* :test #'string=)))
                      (translate-expression (first args))
                      (translate-expression (second args)))))))
    (:is-null
     (destructuring-bind (x negated) (rest ast)
       (list (dsl-op (if negated "IS-NOT-NULL" "IS-NULL")) (translate-expression x))))
    (:in
     (destructuring-bind (x items negated) (rest ast)
       (let ((form (list* (dsl-op "IN") (translate-expression x) (mapcar #'translate-expression items))))
         (if negated (list (dsl-op "NOT") form) form))))
    (:between
     (destructuring-bind (x low high negated) (rest ast)
       (let ((form (list (dsl-op "BETWEEN") (translate-expression x)
                         (translate-expression low) (translate-expression high))))
         (if negated (list (dsl-op "NOT") form) form))))
    (:like
     (destructuring-bind (x pattern negated) (rest ast)
       (let ((form (list (dsl-op "LIKE") (translate-expression x) (translate-expression pattern))))
         (if negated (list (dsl-op "NOT") form) form))))
    (:call
     (destructuring-bind (name args distinct star) (rest ast)
       (translate-call name args distinct star)))))

;; Bind the parameters and check that there are as many as the statement has.
(defmacro with-sql-params ((params count) &body body)
  `(let ((*params* (coerce ,params 'simple-vector)))
     (unless (= ,count (length *params*))
       (sql-fail *sql-text* nil "The statement has ~D ? but ~D value~:P given"
                 ,count (length *params*)))
     ,@body))

;; An expression of SQL, written as text, as an expression for :where and the like.
;; The names in it are columns of TABLE. A ? takes the next of PARAMS.
;;   (select :from "people" :where (sql-expression "age >= ? AND name LIKE 'a%'" "people" 18))
(defun sql-expression (text table &rest params)
  (multiple-value-bind (ast count) (sql-parse-expression text)
    (let ((*sql-text* text)
          (*scope* (scope-for-table (if (typep table 'table) (name table) table)
                                    (find-table table)))
          (*aliases* nil))
      (with-sql-params (params count)
        (translate-expression ast)))))
