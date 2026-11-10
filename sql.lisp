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

;; The item for a column of the scope, named as SQL names a result column: by the
;; column name, or by table.column when two items would have the same name.
(defun column-item (entry column duplicated)
  (let ((ref (column-keyword entry column)))
    (cond ((eq ref (name column)) (if duplicated
                                      (list (dsl-op "AS") ref (prefixed-name (scope-entry-qualifier entry) (name column)))
                                      ref))
          (duplicated (list (dsl-op "AS") ref (prefixed-name (scope-entry-qualifier entry) (name column))))
          (t (list (dsl-op "AS") ref (name column))))))

(defun scope-entries-for (qualifier)
  (if qualifier
      (let ((entry (find qualifier *scope* :key #'scope-entry-qualifier :test #'string-equal)))
        (unless entry
          (sql-fail *sql-text* nil "Unknown table or alias ~A" qualifier))
        (list entry))
      *scope*))

;; Return the :col list for the items, and the aliases that were given with AS.
(defun translate-select-items (items)
  (if (and (null (rest items)) (eq (first (first items)) :star) (null (second (first items)))
           (null (rest *scope*)))
      (values t nil)
      (let ((records '()))
        ;; first find what each item is, so that a repeated column name can be seen
        (dolist (item items)
          (ecase (first item)
            (:star (dolist (entry (scope-entries-for (second item)))
                     (dolist (column (scope-entry-schema entry))
                       (push (list :plain entry column) records))))
            (:expr
             (destructuring-bind (tree alias) (rest item)
               (if (and (null alias) (eq (first tree) :col))
                   (multiple-value-bind (entry column) (resolve-column-entry (second tree) (third tree))
                     (push (list :plain entry column) records))
                   (push (list :expr tree alias) records))))))
        (setf records (nreverse records))
        (let ((names (loop for record in records
                           when (eq (first record) :plain) collect (name (third record))))
              (translated '())
              (aliases '()))
          (dolist (record records)
            (ecase (first record)
              (:plain
               (destructuring-bind (entry column) (rest record)
                 (push (column-item entry column (> (count (name column) names) 1)) translated)))
              (:expr
               (destructuring-bind (tree alias) (rest record)
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

;;; Joins

;; Check that a side of an ON names a column that exists somewhere. An unqualified name
;; that is in two tables is fine here. The caller says what it means.
(defun resolve-column-entry-or-either (side)
  (destructuring-bind (qualifier name) (rest side)
    (multiple-value-bind (entry column) (find-scope-column *scope* qualifier name)
      (declare (ignore entry))
      (when (and (not (typep column 'column)) (not (eq column :ambiguous)))
        (resolve-column-entry qualifier name)))))

;; The two columns of an ON condition, as (here there): HERE names a column of the tables
;; so far, as the rows name it, and THERE names a column of the new table.
(defun resolve-join-condition (on entry)
  (unless (and (eq (first on) :op) (string= (second on) "=")
               (eq (first (third on)) :col) (eq (first (fourth on)) :col))
    (sql-fail *sql-text* nil "ON must be one equality between two columns, such as ON a.x = b.y"))
  ;; a name that is nowhere gets its own message
  (let ((*scope* (append *scope* (list entry))))
    (dolist (side (list (third on) (fourth on)))
      (resolve-column-entry-or-either side)))
  (flet ((earlier (side)
           (multiple-value-bind (e c) (find-scope-column *scope* (second side) (third side))
             (and e (column-keyword e c))))
         (new (side)
           (multiple-value-bind (e c) (find-scope-column (list entry) (second side) (third side))
             (and e (name c)))))
    (let ((a (third on)) (b (fourth on)))
      (dolist (side (list a b))
        (when (and (null (second side)) (earlier side) (new side))
          (sql-fail *sql-text* nil "Column ~A is in more than one table. Write table.~A"
                    (third side) (third side))))
      (cond ((and (earlier a) (new b)) (values (earlier a) (new b)))
            ((and (earlier b) (new a)) (values (earlier b) (new a)))
            (t (sql-fail *sql-text* nil "ON must compare a column of the tables before ~A with a column of ~A"
                         (scope-entry-qualifier entry) (scope-entry-qualifier entry)))))))

;; Add the joins to *scope* one by one, and return the :joins list for select.
(defun translate-joins (join-trees)
  (let ((joins '()))
    (dolist (join join-trees)
      (destructuring-bind (kind (text quoted alias) on) join
        (let* ((table-name (sql-table-name text quoted))
               (table (find-table table-name))
               (qualifier (or alias text))
               (entry (make-scope-entry qualifier table-name (schema table) qualifier)))
          (when (find qualifier *scope* :key #'scope-entry-qualifier :test #'string-equal)
            (sql-fail *sql-text* nil "Two tables are called ~A. Give one an alias" qualifier))
          (multiple-value-bind (here there) (resolve-join-condition on entry)
            (push (list kind table-name here there :as qualifier) joins))
          (setf *scope* (append *scope* (list entry))))))
    (nreverse joins)))

;; The arguments of select for a SELECT tree.
(defun translate-select (ast)
  (destructuring-bind (&key distinct items from joins where group having order limit offset) (rest ast)
    (destructuring-bind (table-text quoted alias) from
      (let* ((table-name (sql-table-name table-text quoted))
             (table (find-table table-name))
             (*scope* (list (make-scope-entry (or alias table-text) table-name (schema table) nil)))
             (*aliases* nil)
             (plist (list :from table-name))
             (join-args (translate-joins joins)))
        (when join-args (setf (getf plist :joins) join-args))
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
