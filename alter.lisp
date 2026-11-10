;;;; Changing tables, and migrations.
;;;;
;;;; rename-column, drop-column and rename-table rebuild the table from its own
;;;; snapshot with the change applied. The rows, the indexes, the triggers and the
;;;; ttl follow. A view or a trigger function that names the old column or table
;;;; is not changed, and fails when it runs.
;;;;
;;;; migrate runs a function once, and remembers it in the table "_migrations".

(in-package :hash-db)

;;; Rebuilding a table from a changed snapshot. These work on a root and return a new
;;; root, so the same code does the change and replays it from the log.

(defun root-table (root name)
  (let ((object (values (fset:lookup root name))))
    (unless (typep object 'table)
      (error 'no-such-table :table name
                            :message (format nil "The table: ~A does not exist" name)))
    object))

;; Replace the table NAME with the one restored from TRANSFORM applied to its snapshot.
;; The snapshot is (name pk col-specs id-rows index-specs trigger-specs ttl).
(defun root-rewrite-table (root name transform)
  (let ((table (restore-table (funcall transform (table-snapshot (root-table root name))))))
    (fset:with (fset:less root name) (name table) table)))

(defun spec-column-name (spec)
  (first spec))

;; A :check expression names its own column. When the column is renamed the
;; expression must name the new one. A quoted constant is left alone.
(defun rename-in-expression (expr old new)
  (cond ((eq expr old) new)
        ((atom expr) expr)
        ((and (symbolp (first expr)) (string= (symbol-name (first expr)) "QUOTE")) expr)
        ((and (symbolp (first expr)) (string= (symbol-name (first expr)) "FN"))
         (list* (first expr) (second expr)
                (mapcar #'(lambda (part) (rename-in-expression part old new)) (cddr expr))))
        (t (cons (first expr)
                 (mapcar #'(lambda (part) (rename-in-expression part old new)) (rest expr))))))

;; The column spec (name type . options) with its :check expression renamed.
(defun rename-spec-check (spec old new)
  (destructuring-bind (name type &rest options) spec
    (let ((check (getf options :check)))
      (if (consp check)
          (let ((options (copy-list options)))
            (setf (getf options :check) (rename-in-expression check old new))
            (list* name type options))
          spec))))

(defun root-rename-column (root table old new)
  (let ((tbl (root-table root table)))
    (let ((old (name (find-column old (schema tbl))))
          (new (as-keyword new)))
      (when (find new (schema tbl) :key #'name)
        (error 'schema-error
               :message (format nil "Table ~A already has a column ~A" table new)))
      (root-rewrite-table
       root table
       #'(lambda (snapshot)
           (destructuring-bind (name pk specs id-rows index-specs trigger-specs ttl) snapshot
             (flet ((rename (key) (if (eq key old) new key)))
               (list name pk
                     (mapcar #'(lambda (spec)
                                 (let ((spec (rename-spec-check spec old new)))
                                   (cons (rename (first spec)) (rest spec))))
                             specs)
                     (mapcar #'(lambda (id-row)
                                 (cons (car id-row)
                                       (loop for (key value) on (cdr id-row) by #'cddr
                                             append (list (rename key) value))))
                             id-rows)
                     (mapcar #'(lambda (spec) (list (first spec) (rename (second spec)) (third spec)))
                             index-specs)
                     trigger-specs
                     (and ttl (rename ttl))))))))))

(defun root-drop-column (root table column)
  (let* ((tbl (root-table root table))
         (col (find-column column (schema tbl)))
         (column (name col)))
    (when (typep col 'primary-key-column)
      (error 'schema-error
             :message (format nil "Column ~A is the primary key of table ~A and can't be dropped" column table)))
    (when (null (rest (schema tbl)))
      (error 'schema-error
             :message (format nil "Column ~A is the only column of table ~A" column table)))
    (root-rewrite-table
     root table
     #'(lambda (snapshot)
         (destructuring-bind (name pk specs id-rows index-specs trigger-specs ttl) snapshot
           (list name pk
                 (remove column specs :key #'first)
                 (mapcar #'(lambda (id-row)
                             (cons (car id-row)
                                   (loop for (key value) on (cdr id-row) by #'cddr
                                         unless (eq key column) append (list key value))))
                         id-rows)
                 (remove column index-specs :key #'second)
                 trigger-specs
                 (and (not (eq ttl column)) ttl)))))))

;; Point every foreign key that names OLD at NEW.
(defun retarget-spec (spec old new)
  (destructuring-bind (name type &rest options) spec
    (if (and (eq type 'foreign-key-column) (equal (getf options :references) old))
        (list* name type (progn (setf options (copy-list options))
                                (setf (getf options :references) new)
                                options))
        spec)))

(defun root-rename-table (root old new)
  (root-table root old)
  (when (nth-value 1 (fset:lookup root new))
    (error 'schema-error :message (format nil "Can't rename ~A to ~A: the name is already used" old new)))
  (let ((root (root-rewrite-table
               root old
               #'(lambda (snapshot)
                   (list* new (rest snapshot))))))
    ;; every table that refers to it, the renamed table too
    (dolist (tbl (all-tables root) root)
      (when (some #'(lambda (col)
                      (and (typep col 'foreign-key-column) (equal (references col) old)))
                  (schema tbl))
        (setf root (root-rewrite-table
                    root (name tbl)
                    #'(lambda (snapshot)
                        (destructuring-bind (name pk specs &rest more) snapshot
                          (list* name pk
                                 (mapcar #'(lambda (spec) (retarget-spec spec old new)) specs)
                                 more)))))))))

;;; The calls

;; Rename a column. The rows, indexes, unique and foreign key checks, and the ttl follow.
(defun rename-column (table old new)
  (with-transaction
    (let ((tbl (stored-table table)))
      (setf (txn-root *txn*) (root-rename-column (txn-root *txn*) (name tbl) old new))
      (journal-add (list :rename-column (name tbl) (as-keyword old) (as-keyword new)))
      (as-keyword new))))

;; Drop a column and its data, and the indexes on it.
(defun drop-column (table column)
  (with-transaction
    (let ((tbl (stored-table table)))
      (setf (txn-root *txn*) (root-drop-column (txn-root *txn*) (name tbl) column))
      (journal-add (list :drop-column (name tbl) (as-keyword column)))
      t)))

;; Rename a table. The foreign keys of other tables that refer to it follow.
(defun rename-table (old new)
  (with-transaction
    (let ((tbl (stored-table old)))
      (setf (txn-root *txn*) (root-rename-table (txn-root *txn*) (name tbl) new))
      (journal-add (list :rename-table (name tbl) new))
      new)))

(define-replay :rename-column (root table old new)
  (root-rename-column root table old new))

(define-replay :drop-column (root table column)
  (root-drop-column root table column))

(define-replay :rename-table (root old new)
  (root-rename-table root old new))

;;; Migrations

(defparameter *migrations-table* "_migrations")

(defun migration-id (id)
  (if (stringp id) id (string-downcase (string id))))

(defun ensure-migrations-table ()
  (unless (get-named-table *migrations-table*)
    (make-table *migrations-table*
                '((:id primary-key-column) (:name string :unique t :not-null t) (:applied timestamp)))))

;; Run FUNCTION once for the migration ID, in one transaction with the note that
;; it ran. Return :applied, or :skipped if it ran before. If FUNCTION fails,
;; nothing it did stays, and the migration is not noted. Put the migrations in a
;; list, in order, and call them each time the program starts.
(defun migrate (id function)
  (let ((id (migration-id id)))
    (with-transaction
      (ensure-migrations-table)
      (cond ((row-list (select :from *migrations-table* :where (list '= :name id)))
             :skipped)
            (t
             (funcall function)
             (insert-row (list :name id :applied (now)) *migrations-table*)
             :applied)))))

;; Run migrations given as a list of (id function). Return the ids that ran now.
(defun migrate-all (migrations)
  (loop for (id function) in migrations
        when (eq :applied (migrate id function))
          collect (migration-id id)))

;; The ids of the migrations that ran, in the order they ran.
(defun applied-migrations ()
  (if (get-named-table *migrations-table*)
      (mapcar #'(lambda (row) (getf row :name))
              (row-list (select :from *migrations-table* :order-by :id)))
      nil))
