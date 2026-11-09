;;;; In-memory tables with primary-key and foreign-key constraints,
;;;; saved to flat files.
;;;; depends on cl-store, BT, alexandria, iterate and make-hash
;;;; v.1

(in-package :hash-db)

;; Return a t or nil on thing output.
;; when I dont need the output, just a truthiness.
(defun !null (thing)
  (not (null thing)))

;; Default #'equal for hash-tables
(defun create-hash-table (&key (test #'equal))
  (make-hash:make-hash :test test))

;; Column names are always keywords: "a-col", a-col and :a-col all mean :A-COL.
(defun as-keyword (designator)
  (if (keywordp designator)
      designator
      (make-keyword (string-upcase (string designator)))))

;; Locks. File lock: one save or load at a time.
;; Tables lock: guards the global table registry.
(defparameter *file-lock*   (bt:make-lock "FILE-LOCK"))
(defparameter *tables-lock* (bt:make-lock "TABLES-LOCK"))

;; Internal locking on tables.
;; The tables lock is a slot
(defmacro get-lock (lock &body body)
  `(bt:with-lock-held (,lock)
     ,@body))

; Global database table
(defparameter *global-tables* (create-hash-table))

(defun show-tables ()
  (hash-table-keys *global-tables*))

;; Return a table of NAME, or nil
(defun get-named-table (name)
  (gethash name *global-tables*))

;; Accept a table name or a table object (for example a select result).
(defun find-table (designator)
  (cond ((typep designator 'table) designator)
        ((get-named-table designator))
        (t (error "The table: ~A does not exist" designator))))

;; Push a table to the database. A duplicate name is an error.
(defun add-table-to-global (db name)
  (get-lock *tables-lock*
    (when (nth-value 1 (gethash name *global-tables*))
      (error "The table: ~A already exists" name))
    (setf (gethash name *global-tables*) db)))

(defun drop-table (name)
  (get-lock *tables-lock*
    (remhash name *global-tables*)))

(defun make-rows ()
  (create-hash-table))

;; A table defined with a unique name and schema
;; table locking is implemented and rows are plists in a hash-table
;; keyed by an integer row id.
(defclass table ()
  ((primary-key   :accessor pk
                  :initarg         :pk
                  :initform 0
                  :type integer)
   (name          :accessor name
                  :initarg         :name)
   (row           :accessor rows
                  :initarg         :rows
                  :initform        (make-rows))
   (schema        :accessor schema
                  :initarg         :schema
                  :initform        nil)
   (lock          :accessor lock
                  :initarg         :lock
                  :initform        (bt:make-lock "TABLE-LOCK")))
  (:documentation "A table in a database."))

(defmethod get-schema ((table table))
  (schema table))

;; Make an instance of a table with a hash-table rowset
(defun create-table-instance (name schema)
  (make-instance 'table :name name :schema schema :lock (bt:make-lock (princ-to-string name))))

(defun not-nullable (val col)
  (or val (error "Column ~A can't be null" (name col))))

(defun keep-value (val col)
  (declare (ignore col))
  val)

(defclass column ()
  ((name                :reader name
                        :initarg                    :name)
   ;; (name type default-value references), used to rebuild the column on load
   (spec                :reader spec
                        :initarg                    :spec
                        :initform nil)
   (equality-predicate  :reader equality-predicate
                        :initarg                    :equality-predicate
                        :initform #'equal)
   (comparator          :reader comparator
                        :initarg                    :comparator
                        :initform nil)
   (default-value       :reader default-value
                        :initarg                    :default-value
                        :initform nil)
   (value-normaliser    :reader value-normaliser
                        :initarg                    :value-normaliser
                        :initform #'keep-value))
  (:documentation "The column definition for a table column"))

(defclass primary-key-column (column)
  ((equality-predicate :initform #'=)
   (value-normaliser   :initform #'not-nullable)))

(defclass foreign-key-column (column)
  ((references         :reader references
                       :initarg :references
                       :initform nil)
   (foreign-column     :reader foreign-column
                       :initform :primary-key)
   (equality-predicate :initform #'=)
   (value-normaliser   :initform #'not-nullable)))

(defmethod initialize-instance :after ((col foreign-key-column) &key)
  (unless (references col)
    (error "Must supply a reference table for the foreign key column ~A." (name col))))

(defclass interned-values-column (column)
  ((interned-values    :reader interned-values
                       :initform (create-hash-table))
   (equality-predicate :initform #'eql)
   (value-normaliser   :initform #'intern-for-column)))

(defun intern-for-column (val col)
  (let ((hash (interned-values col)))
    (or (gethash (not-nullable val col) hash)
        (setf (gethash val hash) val))))

;; A foreign key value must be the id of a row in the referenced table.
(defun check-foreign-key (table-name fk)
  (let ((target (get-named-table table-name)))
    (if (and target (nth-value 1 (gethash fk (rows target))))
        fk
        (error "Foreign key constraint violation: no row ~A in table ~A" fk table-name))))

(defgeneric make-col (name type &optional default-value references))

(defmethod make-col (name type &optional default-value references)
  (declare (ignore name default-value references))
  (error "Unknown column type: ~A" type))

;; Create a class specific column to store instances of that class
;;
(defmacro make-class-specific-col (&key type
                                   class
                                   (comparator       nil comparator-p)
                                   (eq-pred          nil eq-pred-p)
                                   (val-norm         nil val-norm-p)
                                   (references       nil references-p))
  (declare (ignore references))
  `(defmethod make-col (name (type (eql ',type)) &optional default-value references)
     (declare (ignorable type references))
     (make-instance
       ',class
       :name (as-keyword name)
       :spec (list (as-keyword name) ',type default-value references)
       ,@(when comparator-p       `(:comparator ,comparator))
       ,@(when eq-pred-p          `(:equality-predicate ,eq-pred))
       ,@(when val-norm-p         `(:value-normaliser ,val-norm))
       ,@(when references-p       `(:references references))
       :default-value default-value)))


;; A couple default column definitions
;;
(make-class-specific-col :class column
                         :type boolean
                         :comparator #'(lambda (a b) (and (not a) b))
                         :eq-pred #'eq)

(make-class-specific-col :class primary-key-column
                         :type primary-key-column
                         :comparator #'<)

(make-class-specific-col :class foreign-key-column
                         :type foreign-key-column
                         :comparator #'<
                         :references t)

(make-class-specific-col :class column
                         :type string
                         :comparator #'string<
                         :eq-pred #'string=)

(make-class-specific-col :class column
                         :type number
                         :comparator #'<
                         :eq-pred #'=)

(make-class-specific-col :class interned-values-column
                         :type interned-string
                         :comparator #'string<)

(defun make-schema (spec)
  (mapcar #'(lambda (col-spec) (apply #'make-col col-spec)) spec))

(defun validate-schema (table-name schema)
  (let ((names (mapcar #'name schema)))
    (unless (= (length names) (length (remove-duplicates names)))
      (error "Duplicate column names in table ~A: ~A" table-name names))
    (when (> (count-if (lambda (c) (typep c 'primary-key-column)) schema) 1)
      (error "Table ~A has more than one primary key column" table-name))))

(defun make-table (name schema)
  (let ((schema (if (every (lambda (c) (typep c 'column)) schema)
                    schema
                    (make-schema schema))))
    (validate-schema name schema)
    (add-table-to-global (create-table-instance name schema) name)))

;; Value for COL as stored: foreign keys must exist, then the column normaliser runs.
(defun resolve-column-value (col val)
  (when (typep col 'foreign-key-column)
    (check-foreign-key (references col) val))
  (normalise-for-col val col))

(defun normalise-for-col (val col)
  (funcall (value-normaliser col) val col))

(defun find-column (col-name schema)
  (or (find (as-keyword col-name) schema :key #'name)
      (error "No column: ~A in schema: ~A" col-name (mapcar #'name schema))))

(defun check-known-columns (names-and-values schema)
  (unless (evenp (length names-and-values))
    (error "Malformed row, expected :column value pairs: ~S" names-and-values))
  (iter (for key in names-and-values by #'cddr)
        (when (typep (find-column key schema) 'primary-key-column)
          (error "Column ~A is a generated primary key" key))))

(defun supplied-or-default (names-and-values col)
  (let ((val (getf names-and-values (name col) '%missing)))
    (if (eq val '%missing) (default-value col) val)))

(defun normalise-row (names-and-values schema id)
  (check-known-columns names-and-values schema)
  (iter (for col in schema)
        (collect (name col))
        (collect (if (typep col 'primary-key-column)
                     id
                     (resolve-column-value col (supplied-or-default names-and-values col))))))

;; Add a row, return its id. A primary key column holds the id.
(defun insert-row (name-and-values table)
  (let ((tbl (find-table table)))
    (get-lock (lock tbl)
      (let* ((id (1+ (pk tbl)))
             (row (normalise-row name-and-values (schema tbl) id)))
        (setf (pk tbl) id
              (gethash id (rows tbl)) row)
        id))))

(defun get-row (id table)
  (let ((tbl (find-table table)))
    (get-lock (lock tbl)
      (gethash id (rows tbl)))))

;; Change some columns of one row. Rows are replaced, never changed in place.
(defun update-row (id names-and-values table)
  (let ((tbl (find-table table)))
    (get-lock (lock tbl)
      (let ((old (gethash id (rows tbl))))
        (unless old
          (error "No row ~A in table: ~A" id (name tbl)))
        (check-known-columns names-and-values (schema tbl))
        (let ((new (copy-list old)))
          (iter (for (key val) on names-and-values by #'cddr)
                (setf (getf new (as-keyword key))
                      (resolve-column-value (find-column key (schema tbl)) val)))
          (setf (gethash id (rows tbl)) new)
          id)))))

;; Return t if a row was removed.
(defun delete-row (id table)
  (let ((tbl (find-table table)))
    (get-lock (lock tbl)
      (remhash id (rows tbl)))))

;; Add a column to a table. Old rows get the column default.
(defmethod add-column ((table table) col-def)
  (let ((col (apply #'make-col col-def)))
    (when (typep col 'primary-key-column)
      (error "Can't add a primary key column to an existing table"))
    (get-lock (lock table)
      (when (find (name col) (schema table) :key #'name)
        (error "Column ~A already exists in table ~A" (name col) (name table)))
      (let ((val (resolve-column-value col (default-value col))))
        (dolist (id (hash-table-keys (rows table)))
          (setf (gethash id (rows table))
                (append (gethash id (rows table)) (list (name col) val)))))
      (setf (schema table) (append (schema table) (list col)))
      col)))

;; Rows of a table as a list of plists, in id order.
(defun row-list (table)
  (let ((tbl (find-table table)))
    (get-lock (lock tbl)
      (iter (for id in (sort (hash-table-keys (rows tbl)) #'<))
            (collect (gethash id (rows tbl)))))))

;; Run a query. WHERE is a function of one row, see MATCHING.
;; INNER-JOIN is (table-name column-here column-there).
;; Return a new table. Its rows are keyed 1..n in result order.
(defun select (&key (col t) from where distinct order-by inner-join)
  (let* ((tbl (find-table from))
         (schema (schema tbl))
         (rows (row-list tbl)))
    (when inner-join
      (multiple-value-setq (rows schema) (join-rows rows schema inner-join)))
    (when where
      (setf rows (restrict-rows rows where)))
    (when order-by
      (setf rows (sorted-rows rows schema (mklist order-by))))
    (unless (eql col t)
      (setf schema (extract-schema (mklist col) schema))
      (setf rows (project-columns rows schema)))
    (when distinct
      (setf rows (distinct-rows rows schema)))
    (make-result-table rows schema)))

(defun make-result-table (rows schema)
  (let ((result (make-instance 'table :schema schema :name (gensym "SELECT"))))
    (iter (for row in rows)
          (for id from 1)
          (setf (gethash id (rows result)) row))
    (setf (pk result) (length rows))
    result))

(defun mklist (thing)
  (if (listp thing) thing (list thing)))

(defun extract-schema (col-names schema)
  (iter (for c in col-names) (collect (find-column c schema))))

(defun restrict-rows (rows where)
  (remove-if-not where rows))

(defun project-columns (rows schema)
  (mapcar (extractor schema) rows))

(defun distinct-rows (rows schema)
  (remove-duplicates rows :test (row-equality-tester schema) :from-end t))

(defun sorted-rows (rows schema order-by)
  (stable-sort (copy-list rows) (row-comparator order-by schema)))

(defun join-rows (rows schema inner-join)
  (destructuring-bind (other-name here there) inner-join
    (let* ((other (find-table other-name))
           (here-col (find-column here schema))
           (there-col (find-column there (schema other)))
           (other-rows (row-list other))
           (clash (intersection (mapcar #'name schema) (mapcar #'name (schema other)))))
      (when clash
        (error "Can't join: both tables have columns named ~A" clash))
      (values (loop for a in rows
                    nconc (loop for b in other-rows
                                when (funcall (equality-predicate here-col)
                                              (getf a (name here-col))
                                              (getf b (name there-col)))
                                  collect (append a b)))
              (append schema (schema other))))))

(defun extractor (schema)
  (let ((names (mapcar #'name schema)))
    #'(lambda (row) (iter (for c in names) (collect c) (collect (getf row c))))))

(defun row-equality-tester (schema)
  (let ((names (mapcar #'name schema))
        (tests (mapcar #'equality-predicate schema)))
    #'(lambda (a b)
        (iter (for name in names)
              (for test in tests)
              (always (funcall test (getf a name) (getf b name)))))))

;; True when row A sorts before row B. The first column that differs decides.
(defun row-comparator (col-names schema)
  (let* ((names (mapcar #'as-keyword col-names))
         (comparators (mapcar #'(lambda (col)
                                  (or (comparator col)
                                      (error "Column ~A has no comparator" (name col))))
                              (extract-schema names schema))))
    #'(lambda (a b)
        (loop for name in names
              for comp in comparators
              do (let ((a-v (getf a name))
                       (b-v (getf b name)))
                   (cond ((funcall comp a-v b-v) (return t))
                         ((funcall comp b-v a-v) (return nil))))
              finally (return nil)))))

(defun column-matcher (col val)
  (let ((name (name col))
        (predicate (equality-predicate col))
        (normalised (normalise-for-col val col)))
    #'(lambda (row)
        (let ((row-val (getf row name)))
          ;; a null column value never matches a given value
          (and row-val (funcall predicate row-val normalised))))))

;; A nil value means "any value".
(defun column-matchers (schema names-and-values)
  (iter (for (name val) on names-and-values by #'cddr)
        (when val
          (collect (column-matcher (find-column name schema) val)))))

;; Build a WHERE function: (matching "table" :col value ...)
(defun matching (table &rest names-and-values)
  (let ((matchers (column-matchers (schema (find-table table)) names-and-values)))
    #'(lambda (row) (every #'(lambda (matcher) (funcall matcher row)) matchers))))

;;; Persistence. A file holds plain data: table names, row ids, rows and
;;; column specs. Locks and functions are rebuilt on load.

(defparameter +file-format-version+ 1)

(defun table-snapshot (tbl)
  (get-lock (lock tbl)
    (list (name tbl)
          (pk tbl)
          (mapcar #'spec (schema tbl))
          (iter (for (id row) in-hashtable (rows tbl))
                (collect (cons id row))))))

(defun restore-table (snapshot)
  (destructuring-bind (name pk col-specs id-rows) snapshot
    (let ((tbl (create-table-instance name (make-schema col-specs))))
      (setf (pk tbl) pk)
      (dolist (id-row id-rows)
        (setf (gethash (car id-row) (rows tbl)) (cdr id-row)))
      tbl)))

;; Write all tables to FILE. Return FILE.
(defun save-db (file)
  (get-lock *file-lock*
    (let ((snapshots (get-lock *tables-lock*
                       (mapcar #'table-snapshot (hash-table-values *global-tables*)))))
      (cl-store:store (list :hash-db +file-format-version+ snapshots) file)
      file)))

;; Replace all tables with those in FILE. Return the table names.
(defun load-db (file)
  (get-lock *file-lock*
    (let ((data (cl-store:restore file)))
      (unless (and (consp data)
                   (eq (first data) :hash-db)
                   (eql (second data) +file-format-version+))
        (error "~A is not a hash-db file" file))
      (let ((tables (mapcar #'restore-table (third data))))
        (get-lock *tables-lock*
          (clrhash *global-tables*)
          (dolist (tbl tables)
            (setf (gethash (name tbl) *global-tables*) tbl)))
        (show-tables)))))
