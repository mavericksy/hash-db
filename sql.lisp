;;;; SQL: running statements.
;;;;
;;;;   (sql "SELECT name, age FROM people WHERE age > ? ORDER BY name LIMIT 10" 18)
;;;;   (sql-rows "SELECT count(*) AS n FROM people")
;;;;
;;;; A statement is parsed, its names are looked up in the tables, and it becomes calls of
;;;; select, update-where and the rest, so everything that holds for those holds here:
;;;; transactions, indexes, triggers, the log. A ? takes the next of the values given
;;;; after the text, and a value is data, never SQL.
;;;;
;;;; Names are not case sensitive. A table name that is not "quoted" is read in lower
;;;; case, and also matches a table that has capitals if no other table matches.

(in-package :hash-db)

;;; Names

(defun object-names ()
  (fset:convert 'list (fset:domain (current-root))))

;; The name of the table or view that a name in SQL means.
(defun sql-table-name (text quoted)
  (if quoted
      text
      (let ((folded (string-downcase text)))
        (if (nth-value 1 (fset:lookup (current-root) folded))
            folded
            (let ((matches (remove-if-not #'(lambda (name) (and (stringp name) (string-equal name text)))
                                          (object-names))))
              (if (= 1 (length matches)) (first matches) folded))))))

;;; SELECT

;; A number from a LIMIT or an OFFSET.
(defun translate-count (ast what)
  (let ((value (translate-expression ast)))
    (unless (typep value '(integer 0))
      (sql-fail *sql-text* nil "~A must be a whole number, 0 or more" what))
    value))

;; The item for a column of the scope, named as SQL names a result column.
(defun column-item (entry column duplicated)
  (let ((ref (column-keyword entry column)))
    (cond ((eq ref (name column)) ref)
          (duplicated (list (dsl-op "AS") ref (prefixed-name (scope-entry-qualifier entry) (name column))))
          (t (list (dsl-op "AS") ref (name column))))))

(defun scope-entries-for (qualifier)
  (if qualifier
      (let ((entry (find qualifier *scope* :key #'scope-entry-qualifier :test #'string-equal)))
        (unless entry
          (sql-fail *sql-text* nil "Unknown table or alias ~A" qualifier))
        (list entry))
      *scope*))

(defun translate-select-items (items)
  "Return the :col list, and the list of alias keywords."
  (let ((all-star-only (and (null (rest items)) (eq (first (first items)) :star))))
    (if all-star-only
        (values t nil)
        (let ((translated '()) (aliases '()))
          (dolist (item items)
            (ecase (first item)
              (:star
               (dolist (entry (scope-entries-for (second item)))
                 (dolist (column (scope-entry-schema entry))
                   (push (column-item entry column nil) translated))))
              (:expr
               (destructuring-bind (tree alias) (rest item)
                 (let ((expr (translate-expression tree)))
                   (cond (alias
                          (let ((key (as-keyword alias)))
                            (push (list (dsl-op "AS") expr key) translated)
                            (push key aliases)))
                         (t (push expr translated))))))))
          (values (nreverse translated) (nreverse aliases))))))

(defun translate-order-key (key)
  (destructuring-bind (tree direction nulls) key
    (when (and (eq (first tree) :lit) (integerp (second tree)))
      (sql-fail *sql-text* nil "ORDER BY a column number is not supported. Name the column"))
    (let ((expr (translate-expression tree)))
      (if (and (eq direction :asc) (null nulls))
          expr
          (list* (dsl-op (if (eq direction :desc) "DESC" "ASC")) expr
                 (and nulls (list :nulls nulls)))))))

;; The arguments of select for a SELECT tree.
(defun translate-select (ast)
  (destructuring-bind (&key distinct items from joins where group having order limit offset) (rest ast)
    (declare (ignore joins))
    (destructuring-bind (table-text quoted alias) from
      (let* ((table-name (sql-table-name table-text quoted))
             (table (find-table table-name))
             (*scope* (list (make-scope-entry (or alias table-text) table-name (schema table) nil)))
             (*aliases* nil)
             (plist (list :from table-name)))
        (multiple-value-bind (col aliases) (translate-select-items items)
          (setf (getf plist :col) col)
          (when distinct (setf (getf plist :distinct) t))
          (when where (setf (getf plist :where) (translate-expression where)))
          (when group (setf (getf plist :group-by) (mapcar #'translate-expression group)))
          (let ((*aliases* aliases))
            (when having (setf (getf plist :having) (translate-expression having)))
            (when order (setf (getf plist :order-by) (mapcar #'translate-order-key order))))
          (when limit (setf (getf plist :limit) (translate-count limit "LIMIT")))
          (when offset (setf (getf plist :offset) (translate-count offset "OFFSET")))
          plist)))))

;;; Running

(defun execute-statement (ast)
  (ecase (first ast)
    (:select (apply #'select (translate-select ast)))))

;; Run one statement of SQL and return its result. The ? in it take the values after TEXT.
;; A SELECT gives a table, so use row-list or sql-rows to read it.
(defun sql (text &rest params)
  (multiple-value-bind (statements count) (sql-parse text)
    (when (rest statements)
      (sql-fail text nil "sql runs one statement. Use sql-script for several"))
    (let ((*sql-text* text))
      (with-sql-params (params count)
        (with-transaction
          (execute-statement (first statements)))))))

;; Run a SELECT and return its rows as a list of plists.
(defun sql-rows (text &rest params)
  (row-list (apply #'sql text params)))
