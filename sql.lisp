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

;; A table reference of a FROM or a JOIN. Return what select takes for it (a table name, or
;; the table of a query in brackets), the table, and the name a query uses for it.
(defun resolve-table-reference (reference)
  (if (eq (first reference) :derived)
      (destructuring-bind (query alias) (rest reference)
        (let ((table (run-subquery query)))
          (values table table alias)))
      (destructuring-bind (text quoted alias) reference
        (let ((table-name (sql-table-name text quoted)))
          (values table-name (find-table table-name) (or alias text))))))

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
      (destructuring-bind (kind reference on) join
        (multiple-value-bind (table-name table qualifier) (resolve-table-reference reference)
          (let ((entry (make-scope-entry qualifier table-name (schema table) qualifier)))
            (when (find qualifier *scope* :key #'scope-entry-qualifier :test #'string-equal)
              (sql-fail *sql-text* nil "Two tables are called ~A. Give one an alias" qualifier))
            (multiple-value-bind (here there) (resolve-join-condition on entry)
              (push (list kind table-name here there :as qualifier) joins))
            (setf *scope* (append *scope* (list entry)))))))
    (nreverse joins)))

;; The arguments of select for a SELECT tree.
;; A :where or :having of nil means no condition, but a condition that is the constant
;; false must keep every row out.
(defun condition-argument (expr)
  (if (null expr) (list (dsl-op "QUOTE") nil) expr))

(defun translate-select (ast)
  (destructuring-bind (&key distinct items from joins where group having order limit offset) (rest ast)
    (multiple-value-bind (table-name table qualifier) (resolve-table-reference from)
      (let* ((*scope* (list (make-scope-entry qualifier table-name (schema table) nil)))
             (*aliases* nil)
             (plist (list :from table-name))
             (join-args (translate-joins joins)))
        (when join-args (setf (getf plist :joins) join-args))
        (multiple-value-bind (col aliases) (translate-select-items items)
          (setf (getf plist :col) col)
          (when distinct (setf (getf plist :distinct) t))
          (when where (setf (getf plist :where) (condition-argument (translate-expression where))))
          (when group (setf (getf plist :group-by) (mapcar #'translate-expression group)))
          (let ((*aliases* aliases))
            (when having (setf (getf plist :having) (condition-argument (translate-expression having))))
            (when order (setf (getf plist :order-by) (mapcar #'translate-order-key order))))
          (when limit (setf (getf plist :limit) (translate-count limit "LIMIT")))
          (when offset (setf (getf plist :offset) (translate-count offset "OFFSET")))
          plist)))))

;;; Running

;;; INSERT, UPDATE and DELETE

;; The value of an expression that names no column, such as 1 + 2 or ?.
(defun constant-sql-value (tree)
  (let ((*scope* nil)
        (*aliases* nil))
    (materialize (funcall (compile-expr (translate-expression tree) (make-ectx nil)) nil))))

;; The column that a name means in TBL.
(defun table-column (tbl name)
  (or (sql-find-column name (schema tbl))
      (sql-fail *sql-text* nil "Table ~A has no column ~A" (name tbl) name)))

(defun insert-columns (tbl names)
  (let ((columns (if names
                     (mapcar #'(lambda (name) (table-column tbl name)) names)
                     (remove-if #'(lambda (col) (typep col 'primary-key-column)) (schema tbl)))))
    (let ((keys (mapcar #'name columns)))
      (unless (= (length keys) (length (remove-duplicates keys)))
        (sql-fail *sql-text* nil "A column is named twice in the INSERT")))
    columns))

(defun execute-insert (ast)
  (destructuring-bind (table-reference names rows select) (rest ast)
    (destructuring-bind (table-text quoted alias) table-reference
      (declare (ignore alias))
      (let* ((table-name (sql-table-name table-text quoted))
             (tbl (stored-table table-name))
             (columns (insert-columns tbl names))
             (count 0)
             (last-id nil))
        (flet ((add (pairs)
                 (setf last-id (insert-row pairs table-name))
                 (incf count)))
          (if select
              (let ((result (run-query select)))
                (unless (= (length (schema result)) (length columns))
                  (sql-fail *sql-text* nil "The INSERT names ~D column~:P but the SELECT gives ~D"
                            (length columns) (length (schema result))))
                (dolist (row (row-list result))
                  (add (loop for column in columns
                             for source in (schema result)
                             append (list (name column) (getf row (name source)))))))
              (dolist (row rows)
                (unless (= (length row) (length columns))
                  (sql-fail *sql-text* nil "A row has ~D value~:P but the INSERT names ~D column~:P"
                            (length row) (length columns)))
                (add (loop for column in columns
                           for tree in row
                           unless (eq (first tree) :default)
                             append (list (name column) (constant-sql-value tree)))))))
        (values count last-id)))))

(defun execute-update (ast)
  (destructuring-bind (table-reference assignments where) (rest ast)
    (destructuring-bind (table-text quoted alias) table-reference
      (let* ((table-name (sql-table-name table-text quoted))
             (tbl (stored-table table-name))
             (*scope* (list (make-scope-entry (or alias table-text) table-name (schema tbl) nil)))
             (*aliases* nil)
             (compute (loop for (column tree) in assignments
                            append (list (name (table-column tbl column)) (translate-expression tree)))))
        (update-where table-name nil :compute compute
                                     :where (if where (translate-expression where) t))))))

(defun execute-delete (ast)
  (destructuring-bind (table-reference where) (rest ast)
    (destructuring-bind (table-text quoted alias) table-reference
      (let* ((table-name (sql-table-name table-text quoted))
             (tbl (stored-table table-name))
             (*scope* (list (make-scope-entry (or alias table-text) table-name (schema tbl) nil)))
             (*aliases* nil))
        (delete-where table-name :where (if where (translate-expression where) t))))))

;;; CREATE, DROP and ALTER

;; The name of a table, view or sequence that a statement makes. It is read in lower
;; case unless it is "quoted".
(defun sql-new-name (text quoted)
  (if quoted text (string-downcase text)))

(defparameter *sql-types*
  '(("INTEGER" . integer) ("INT" . integer) ("BIGINT" . integer) ("SMALLINT" . integer)
    ("TINYINT" . integer)
    ("REAL" . float) ("FLOAT" . float) ("DOUBLE" . float)
    ("NUMERIC" . number) ("DECIMAL" . number) ("NUMBER" . number)
    ("TEXT" . string) ("VARCHAR" . string) ("CHAR" . string) ("CHARACTER" . string)
    ("STRING" . string) ("CLOB" . string)
    ("BOOLEAN" . boolean) ("BOOL" . boolean)
    ("TIMESTAMP" . timestamp) ("DATETIME" . timestamp) ("DATE" . timestamp)
    ("JSON" . json) ("JSONB" . json) ("KEYWORD" . keyword) ("ANY" . any)))

(defun sql-column-type (name)
  (or (cdr (assoc name *sql-types* :test #'string=))
      (sql-fail *sql-text* nil "The column type ~A is not supported. Supported: ~{~A~^, ~}"
                name (mapcar #'car *sql-types*))))

;; One column while a CREATE TABLE is read. OPTIONS is a plist for make-col.
(defstruct (colspec (:constructor make-colspec (name type options)))
  name
  type
  options)

(defun colspec-set (colspec option value)
  (setf (getf (colspec-options colspec) option) value))

;; The primary key a table has, or will have, as a keyword. SELF is the table being made.
(defun referenced-key (table-name column-name self-name self-key)
  (let ((key (if (equal table-name self-name)
                 self-key
                 (let ((object (named-object table-name)))
                   (unless (typep object 'table)
                     (error 'no-such-table :table table-name
                                           :message (format nil "The table: ~A does not exist" table-name)))
                   (let ((pk (find-if #'(lambda (c) (typep c 'primary-key-column)) (schema object))))
                     (and pk (name pk)))))))
    (unless key
      (sql-fail *sql-text* nil "Table ~A has no primary key, so a foreign key can not refer to it" table-name))
    (when (and column-name (not (eq (as-keyword column-name) key))
               (not (eq (as-keyword (substitute #\- #\_ column-name)) key)))
      (sql-fail *sql-text* nil "A foreign key refers to the primary key of ~A, which is ~A, not ~A"
                table-name key column-name))
    key))

(defun make-reference (colspec references self-name self-key)
  (destructuring-bind (table quoted column action) (rest references)
    (unless (eq (colspec-type colspec) 'integer)
      (sql-fail *sql-text* nil "A column with REFERENCES must be an integer column, not ~A" (colspec-name colspec)))
    (let ((table-name (sql-table-name table quoted)))
      (referenced-key table-name column self-name self-key)
      (setf (colspec-type colspec) 'foreign-key-column)
      (colspec-set colspec :references table-name)
      (colspec-set colspec :on-delete action))))

;; The expression of hash-db for a CHECK tree about one column.
(defun translate-check (tree column-key)
  (let ((*scope* (list (make-scope-entry "" nil (list (make-result-column column-key)) nil)))
        (*aliases* nil))
    (translate-expression tree)))

;; The columns that a tree names.
(defun tree-columns (tree)
  (cond ((atom tree) nil)
        ((eq (first tree) :col) (list (third tree)))
        (t (loop for part in (rest tree) append (and (consp part) (if (keywordp (first part))
                                                                        (tree-columns part)
                                                                        (loop for p in part append (tree-columns p))))))))

(defun colspec-from-definition (ast)
  (destructuring-bind (name type constraints) (rest ast)
    (let ((colspec (make-colspec (as-keyword name) (sql-column-type type) nil))
          (checks '()))
      (dolist (constraint constraints)
        (ecase (first constraint)
          (:not-null (colspec-set colspec :not-null t))
          (:null nil)
          (:primary-key
           (unless (eq (colspec-type colspec) 'integer)
             (sql-fail *sql-text* nil "A PRIMARY KEY must be an INTEGER column. Ids are generated"))
           (setf (colspec-type colspec) 'primary-key-column))
          (:unique (colspec-set colspec :unique t))
          (:default
           (let ((tree (second constraint)))
             (if (and (eq (first tree) :call) (string= (second tree) "NOW"))
                 (colspec-set colspec :default-fn 'now)
                 (colspec-set colspec :default (constant-sql-value tree)))))
          (:check (push (second constraint) checks))
          (:references nil)))
      (values colspec (nreverse checks)
              (find :references constraints :key #'first)))))

;; Fold a list of CHECK trees into one expression for a column.
(defun combined-check (trees column-key)
  (let ((checks (mapcar #'(lambda (tree) (translate-check tree column-key)) trees)))
    (if (rest checks) (cons (dsl-op "AND") checks) (first checks))))

(defun colspec-spec (colspec)
  (list* (colspec-name colspec) (colspec-type colspec) (colspec-options colspec)))

(defun execute-create-table (ast)
  (destructuring-bind (name quoted if-not-exists elements) (rest ast)
    (let ((table-name (sql-new-name name quoted)))
      (if (and if-not-exists (nth-value 1 (fset:lookup (current-root) table-name)))
          nil
          (let ((colspecs '()) (checks '()) (references '()) (self-key nil))
            (dolist (element elements)
              (when (eq (first element) :column)
                (multiple-value-bind (colspec column-checks reference) (colspec-from-definition element)
                  (push colspec colspecs)
                  (push (cons colspec column-checks) checks)
                  (when reference (push (cons colspec reference) references)))))
            (setf colspecs (nreverse colspecs))
            (flet ((find-colspec (column-name)
                     (or (find (as-keyword column-name) colspecs :key #'colspec-name)
                         (find (as-keyword (substitute #\- #\_ column-name)) colspecs :key #'colspec-name)
                         (sql-fail *sql-text* nil "The table has no column ~A" column-name))))
              ;; constraints of the table
              (dolist (element elements)
                (case (first element)
                  (:table-primary-key
                   (let ((colspec (find-colspec (second element))))
                     (unless (eq (colspec-type colspec) 'integer)
                       (sql-fail *sql-text* nil "A PRIMARY KEY must be an INTEGER column. Ids are generated"))
                     (setf (colspec-type colspec) 'primary-key-column)))
                  (:table-unique (colspec-set (find-colspec (second element)) :unique t))
                  (:table-foreign-key
                   (push (cons (find-colspec (second element)) (third element)) references))
                  (:table-check
                   (let ((columns (remove-duplicates (tree-columns (second element)) :test #'string-equal)))
                     (unless (= 1 (length columns))
                       (sql-fail *sql-text* nil "A CHECK can name one column of the table, and this names ~D"
                                 (length columns)))
                     (let ((colspec (find-colspec (first columns))))
                       (push (second element) (cdr (assoc colspec checks))))))))
              (when (> (count 'primary-key-column colspecs :key #'colspec-type) 1)
                (sql-fail *sql-text* nil "A table can have one primary key"))
              (setf self-key (let ((pk (find 'primary-key-column colspecs :key #'colspec-type)))
                               (and pk (colspec-name pk))))
              (dolist (reference (reverse references))
                (make-reference (car reference) (cdr reference) table-name self-key))
              (dolist (entry checks)
                (when (cdr entry)
                  (colspec-set (car entry) :check
                               (combined-check (reverse (cdr entry)) (colspec-name (car entry)))))))
            (dolist (colspec colspecs)
              (when (and (eq (colspec-type colspec) 'primary-key-column)
                         (or (getf (colspec-options colspec) :default)
                             (getf (colspec-options colspec) :default-fn)))
                (sql-fail *sql-text* nil "A primary key is generated, so it can have no DEFAULT"))
              (when (eq (colspec-type colspec) 'primary-key-column)
                (setf (colspec-options colspec) nil)))
            (table-name-of (make-table table-name (mapcar #'colspec-spec colspecs))))))))

(defun table-name-of (table)
  (name table))

(defun execute-create-index (ast)
  (destructuring-bind (name unique if-not-exists table quoted column) (rest ast)
    (let* ((table-name (sql-table-name table quoted))
           (tbl (stored-table table-name))
           (key (name (table-column tbl column))))
      (if (and if-not-exists name (find (as-keyword name) (indexes tbl) :key #'index-name))
          nil
          (create-index table-name key :name name :unique unique)))))

(defun execute-create-view (ast)
  (destructuring-bind (name quoted if-not-exists select) (rest ast)
    (let ((view-name (sql-new-name name quoted)))
      (if (and if-not-exists (nth-value 1 (fset:lookup (current-root) view-name)))
          nil
          (let ((*subqueries-allowed* nil))
            (apply #'create-view view-name (translate-select select)))))))

(defun execute-create-sequence (ast)
  (destructuring-bind (name quoted if-not-exists start increment) (rest ast)
    (let ((sequence-name (sql-new-name name quoted)))
      (if (and if-not-exists (nth-value 1 (fset:lookup (current-root) sequence-name)))
          nil
          (create-sequence sequence-name :start start :increment increment)))))

(defun execute-drop (ast)
  (destructuring-bind (kind name quoted if-exists table) (rest ast)
    (ecase kind
      (:table
       (let ((table-name (sql-table-name name quoted)))
         (cond ((typep (named-object table-name) 'table) (drop-table table-name))
               (if-exists nil)
               (t (error 'no-such-table :table table-name
                                        :message (format nil "The table: ~A does not exist" table-name))))))
      (:view
       (let ((view-name (sql-table-name name quoted)))
         (or (drop-view view-name)
             (and (not if-exists) (sql-fail *sql-text* nil "No view named ~A" name)))))
      (:sequence
       (let ((sequence-name (sql-new-name name quoted)))
         (cond ((drop-sequence sequence-name) t)
               (if-exists nil)
               (t (error 'no-such-sequence :name sequence-name
                                           :message (format nil "The sequence: ~A does not exist" sequence-name))))))
      (:index
       (let* ((tables (if table
                          (list (stored-table (sql-table-name table nil)))
                          (all-tables)))
              ;; an SQL name has no hyphen, so age_index also finds AGE-INDEX
              (key (or (find-if #'(lambda (candidate)
                                    (some #'(lambda (tbl) (index-named tbl candidate)) tables))
                                (list (as-keyword name) (as-keyword (substitute #\- #\_ name))))
                       (as-keyword name)))
              (owners (remove-if-not #'(lambda (tbl) (index-named tbl key)) tables)))
         (cond ((null owners)
                (if if-exists nil (sql-fail *sql-text* nil "No index named ~A" name)))
               ((rest owners)
                (sql-fail *sql-text* nil "Index ~A is on more than one table. Write DROP INDEX ~A ON table" name name))
               (t (drop-index key (name (first owners))))))))))

(defun execute-alter-table (ast)
  (destructuring-bind (name quoted action) (rest ast)
    (let* ((table-name (sql-table-name name quoted))
           (tbl (stored-table table-name)))
      (ecase (first action)
        (:add-column
         (multiple-value-bind (colspec checks reference) (colspec-from-definition (second action))
           (when reference
             (make-reference colspec reference table-name nil))
           (when (eq (colspec-type colspec) 'primary-key-column)
             (sql-fail *sql-text* nil "A primary key column can not be added to a table that exists"))
           (when checks
             (colspec-set colspec :check (combined-check checks (colspec-name colspec))))
           (add-column table-name (colspec-spec colspec))
           (colspec-name colspec)))
        (:drop-column
         (drop-column table-name (name (table-column tbl (second action)))))
        (:rename-column
         (rename-column table-name (name (table-column tbl (second action))) (as-keyword (third action))))
        (:rename-table
         (rename-table table-name (sql-new-name (second action) nil)))))))

;; Run a query and give its table.
(defun run-query (ast)
  (ecase (first ast)
    (:select (apply #'select (translate-select ast)))))

;; Run a subquery. It is worked out once, now, and does not see the tables of the query
;; around it, so it can not be correlated with it.
(defun run-subquery (query)
  (unless *subqueries-allowed*
    (sql-fail *sql-text* nil "A view can't have a subquery, because the view keeps the query and not its answer"))
  (let ((*outer-scope* (append *scope* *outer-scope*))
        (*scope* nil)
        (*aliases* nil))
    (run-query query)))

(defun execute-statement (ast)
  (ecase (first ast)
    (:select (run-query ast))
    (:insert (execute-insert ast))
    (:update (execute-update ast))
    (:delete (execute-delete ast))
    (:create-table (execute-create-table ast))
    (:create-index (execute-create-index ast))
    (:create-view (execute-create-view ast))
    (:create-sequence (execute-create-sequence ast))
    (:drop (execute-drop ast))
    (:alter-table (execute-alter-table ast))
    (:explain (apply #'explain (translate-select (second ast))))))

;; EXPLAIN SELECT ... gives the steps of the query, as explain does. Run one statement of SQL and
;; return its result. The ? in it take the values after TEXT.
;; A SELECT gives a table, so use row-list or sql-rows to read it. INSERT gives the number of
;; rows added and the id of the last one. UPDATE and DELETE give the number of rows changed.
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

;; Run several statements, separated by semicolons, as one transaction: all of them
;; happen, or none. The ? of all the statements take the values in order. Return the
;; list of their results. Each statement is read when its turn comes, so a script
;; can make a table and then fill it.
(defun sql-script (text &rest params)
  (multiple-value-bind (statements count) (sql-parse text)
    (let ((*sql-text* text))
      (with-sql-params (params count)
        (with-transaction
          (mapcar #'execute-statement statements))))))
