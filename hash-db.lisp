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

;; File lock: one save or load at a time.
(defparameter *file-lock*   (bt:make-lock "FILE-LOCK"))
;; Writer lock: one writer at a time. A change takes the lock, builds the new
;; state, then swaps it in. Readers never take a lock.
(defparameter *writer-lock* (bt:make-recursive-lock "WRITER-LOCK"))

(defmacro get-lock (lock &body body)
  `(bt:with-lock-held (,lock)
     ,@body))

(defmacro with-writer (&body body)
  `(bt:with-recursive-lock-held (*writer-lock*)
     ,@body))

;;; Storage.
;;; All tables of a database sit in one immutable map: table name -> table.
;;; A table is an immutable value. A change makes a new table and a new map,
;;; and swaps the new map in as the root. Old maps stay valid for readers
;;; that still hold them.

(defstruct (db (:constructor make-db (&optional (root (fset:empty-map)))))
  root)

(defvar *db* (make-db))

;; State of the open transaction in this thread. It holds the working root.
(defstruct (txn (:constructor make-txn (root)))
  root)

(defvar *txn* nil)

;; The fixed root of an open WITH-SNAPSHOT in this thread.
(defvar *snapshot* nil)

;; The root this thread reads: its own working root inside a transaction,
;; else the fixed root of a snapshot, else the last committed root.
(defun current-root ()
  (cond (*txn* (txn-root *txn*))
        (*snapshot*)
        (t (db-root *db*))))

(defun call-with-snapshot (fn)
  (if (or *txn* *snapshot*)
      (funcall fn)
      (let ((*snapshot* (db-root *db*)))
        (funcall fn))))

;; Run BODY against one fixed, committed state of the database. Every read in
;; BODY sees the same data, even if other threads commit meanwhile. Changes
;; made in BODY commit as usual, but BODY does not see them.
;; Inside a transaction this does nothing, because a transaction already
;; reads its own working state.
(defmacro with-snapshot (&body body)
  `(call-with-snapshot (lambda () ,@body)))

;; Run FN as one atomic change. The new root becomes visible only when FN returns.
;; If FN exits any other way, the change is dropped.
;; Inside a transaction, this makes a savepoint. If FN exits any other way,
;; only the changes since the savepoint are dropped.
(defun call-with-transaction (fn)
  (if *txn*
      (call-with-savepoint fn)
      (with-writer
        (let ((*txn* (make-txn (db-root *db*))))
          (catch 'rollback
            (multiple-value-prog1 (funcall fn)
              (setf (db-root *db*) (txn-root *txn*))))))))

(defun call-with-savepoint (fn)
  (let ((saved (txn-root *txn*))
        (done nil))
    (unwind-protect
         (catch 'rollback
           (multiple-value-prog1 (funcall fn)
             (setf done t)))
      (unless done
        (setf (txn-root *txn*) saved)))))

;; Run BODY as one change. All of it happens, or none of it.
;; Return the values of BODY.
(defmacro with-transaction (&body body)
  `(call-with-transaction (lambda () ,@body)))

;; Drop the changes of the innermost WITH-TRANSACTION and leave it.
;; That WITH-TRANSACTION returns nil.
(defun rollback ()
  (unless *txn*
    (error 'transaction-error :message "ROLLBACK called outside a transaction"))
  (throw 'rollback nil))

;; A table is a value. Never change one. Use UPDATED-TABLE and PUT-TABLE.
(defclass table ()
  ((primary-key   :reader pk
                  :initarg         :pk
                  :initform 0
                  :type integer)
   (name          :reader name
                  :initarg         :name)
   ;; fset map: row id -> row plist
   (row           :reader rows
                  :initarg         :rows
                  :initform        (fset:empty-map))
   (schema        :reader schema
                  :initarg         :schema
                  :initform        nil))
  (:documentation "A table in a database."))

(defmethod get-schema ((table table))
  (schema table))

(defun create-table-instance (name schema)
  (make-instance 'table :name name :schema schema))

(defun updated-table (tbl &key (rows (rows tbl)) (pk (pk tbl)) (schema (schema tbl)))
  (make-instance 'table :name (name tbl) :rows rows :pk pk :schema schema))

;; Store TBL in the working root. Only call this inside WITH-TRANSACTION.
(defun put-table (tbl)
  (setf (txn-root *txn*) (fset:with (txn-root *txn*) (name tbl) tbl))
  tbl)

(defun rows-list (rows)
  (mapcar #'cdr (fset:convert 'list rows)))

(defun show-tables ()
  (fset:convert 'list (fset:domain (current-root))))

;; Return a table of NAME, or nil
(defun get-named-table (name)
  (values (fset:lookup (current-root) name)))

(defun all-tables (&optional (root (current-root)))
  (mapcar #'(lambda (name) (values (fset:lookup root name)))
          (fset:convert 'list (fset:domain root))))

;; Accept a table name or a table value (for example a select result).
(defun find-table (designator)
  (cond ((typep designator 'table) designator)
        ((get-named-table designator))
        (t (error 'no-such-table :table designator
                  :message (format nil "The table: ~A does not exist" designator)))))

;; The stored table for a name or table value. Changes need a stored table.
(defun stored-table (designator)
  (let ((name (if (typep designator 'table) (name designator) designator)))
    (or (get-named-table name)
        (error 'no-such-table :table name
               :message (format nil "The table: ~A does not exist" name)))))

;; Add a table to the database. A duplicate name is an error.
(defun add-table (tbl)
  (when (nth-value 1 (fset:lookup (txn-root *txn*) (name tbl)))
    (error 'table-exists :table (name tbl)
           :message (format nil "The table: ~A already exists" (name tbl))))
  (put-table tbl))

(defun drop-table (name)
  (with-transaction
    (when (get-named-table name)
      (let ((refs (remove name (referencing-columns name)
                          :key #'(lambda (ref) (name (car ref))) :test #'equal)))
        (when refs
          (error 'schema-error
                 :message (format nil "Can't drop table ~A: table ~A refers to it"
                                  name (name (car (first refs)))))))
      (setf (txn-root *txn*) (fset:less (txn-root *txn*) name))
      t)))

;;; NULL is the keyword :null, so it never mixes up with false (nil).
(defun null-value-p (val)
  (eq val :null))

(defun keep-value (val col)
  (declare (ignore col))
  val)

(defun to-double (val col)
  (declare (ignore col))
  (coerce val 'double-float))

(defclass column ()
  ((name                :reader name
                        :initarg                    :name)
   ;; (name type . options), used to rebuild the column on load
   (spec                :reader spec
                        :initarg                    :spec
                        :initform nil)
   (equality-predicate  :reader equality-predicate
                        :initarg                    :equality-predicate
                        :initform #'equal)
   (comparator          :reader comparator
                        :initarg                    :comparator
                        :initform nil)
   ;; a CL type specifier. Every value that is not NULL must match it.
   (value-type          :reader value-type
                        :initarg                    :value-type
                        :initform t)
   (not-null            :reader not-null-p
                        :initarg                    :not-null
                        :initform nil)
   (default-value       :reader default-value
                        :initarg                    :default
                        :initform :null)
   ;; function designator called with no arguments for each new row
   (default-fn          :reader default-fn
                        :initarg                    :default-fn
                        :initform nil)
   (unique              :reader unique-p
                        :initarg                    :unique
                        :initform nil)
   ;; function designator called with a value. Nil result rejects the value.
   (check               :reader check-fn
                        :initarg                    :check
                        :initform nil)
   (value-normaliser    :reader value-normaliser
                        :initarg                    :value-normaliser
                        :initform #'keep-value))
  (:documentation "The column definition for a table column"))

(defclass primary-key-column (column)
  ((equality-predicate :initform #'=)
   (not-null           :initform t)))

(defclass foreign-key-column (column)
  ((references         :reader references
                       :initarg :references
                       :initform nil)
   (foreign-column     :reader foreign-column
                       :initform :primary-key)
   ;; What to do with this row when the row it refers to is deleted.
   (on-delete          :reader on-delete
                       :initarg :on-delete
                       :initform :restrict)
   (equality-predicate :initform #'=)))

(defclass interned-values-column (column)
  ((interned-values    :reader interned-values
                       :initform (create-hash-table))
   (equality-predicate :initform #'eql)
   (not-null           :initform t)
   (value-normaliser   :initform #'intern-for-column)))

(defmethod initialize-instance :after ((col foreign-key-column) &key)
  (unless (references col)
    (error 'schema-error
           :message (format nil "Must supply a reference table for the foreign key column ~A." (name col))))
  (unless (member (on-delete col) '(:restrict :cascade :set-null))
    (error 'schema-error
           :message (format nil "Option :on-delete of column ~A must be :restrict, :cascade or :set-null"
                            (name col))))
  (when (and (eq (on-delete col) :set-null) (not-null-p col))
    (error 'schema-error
           :message (format nil "Column ~A can't be :not-null with :on-delete :set-null" (name col)))))

;; A constant default value must pass the same checks as any other value.
(defmethod initialize-instance :after ((col column) &key)
  (flet ((designator-p (x) (typep x '(or function (and symbol (not null))))))
    (dolist (option (list (cons :default-fn (default-fn col)) (cons :check (check-fn col))))
      (when (and (cdr option) (not (designator-p (cdr option))))
        (error 'schema-error
               :message (format nil "Option ~S of column ~A must be a function or a function name"
                                (car option) (name col))))))
  (when (and (default-fn col) (not (null-value-p (default-value col))))
    (error 'schema-error
           :message (format nil "Column ~A has both :default and :default-fn" (name col))))
  (unless (null-value-p (default-value col))
    (normalise-for-col (default-value col) col)))

;; The value for a column that a new row does not give.
(defun column-default (col)
  (if (default-fn col)
      (funcall (default-fn col))
      (default-value col)))

(defun intern-for-column (val col)
  (let ((hash (interned-values col)))
    (or (gethash val hash)
        (setf (gethash val hash) val))))

;; Check a value against a column. NULL passes unless the column is not null.
;; Other values must match the column type, then the normaliser may change them.
(defun normalise-for-col (val col)
  (cond ((null-value-p val)
         (when (not-null-p col)
           (error 'not-null-violation :column (name col)
                  :message (format nil "Column ~A can't be null" (name col))))
         val)
        ((typep val (value-type col))
         (funcall (value-normaliser col) val col))
        (t
         (error 'type-mismatch :column (name col)
                :message (format nil "Column ~A expects ~A, got ~S"
                                 (name col) (value-type col) val)))))

;; A foreign key value must be the id of a row in the referenced table.
(defun check-foreign-key (table-name fk)
  (let ((target (get-named-table table-name)))
    (if (and target (nth-value 1 (fset:lookup (rows target) fk)))
        fk
        (error 'foreign-key-violation :table table-name
               :message (format nil "Foreign key constraint violation: no row ~A in table ~A"
                                fk table-name)))))

(defgeneric make-col (name type &rest options))

(defmethod make-col (name type &rest options)
  (declare (ignore name options))
  (error 'schema-error :message (format nil "Unknown column type: ~A" type)))

(defun check-column-options (type options allowed)
  (unless (evenp (length options))
    (error 'schema-error
           :message (format nil "Column options must be :key value pairs: ~S" options)))
  (iter (for key in options by #'cddr)
        (unless (member key allowed)
          (error 'schema-error
                 :message (format nil "Unknown option ~S for column type ~A. Allowed: ~S"
                                  key type allowed)))))

;; Create a class specific column to store instances of that class.
;; Columns are made with (make-col name type :option value ...)
;;
(defmacro make-class-specific-col (&key type
                                   class
                                   (value-type       t)
                                   (comparator       nil comparator-p)
                                   (eq-pred          nil eq-pred-p)
                                   (val-norm         nil val-norm-p)
                                   (extra-options    nil))
  `(defmethod make-col (name (type (eql ',type)) &rest options)
     (declare (ignorable type))
     (check-column-options ',type options
                          '(:default :default-fn :not-null :unique :check ,@extra-options))
     (apply #'make-instance
            ',class
            :name (as-keyword name)
            :spec (list* (as-keyword name) ',type options)
            :value-type ',value-type
            ,@(when comparator-p       `(:comparator ,comparator))
            ,@(when eq-pred-p          `(:equality-predicate ,eq-pred))
            ,@(when val-norm-p         `(:value-normaliser ,val-norm))
            options)))


;; A couple default column definitions
;;
(make-class-specific-col :class column
                         :type boolean
                         :value-type boolean
                         :comparator #'(lambda (a b) (and (not a) b))
                         :eq-pred #'eq)

(make-class-specific-col :class primary-key-column
                         :type primary-key-column
                         :value-type integer
                         :comparator #'<)

(make-class-specific-col :class foreign-key-column
                         :type foreign-key-column
                         :value-type integer
                         :comparator #'<
                         :extra-options (:references :on-delete))

(make-class-specific-col :class column
                         :type string
                         :value-type string
                         :comparator #'string<
                         :eq-pred #'string=)

(make-class-specific-col :class column
                         :type number
                         :value-type real
                         :comparator #'<
                         :eq-pred #'=)

(make-class-specific-col :class column
                         :type integer
                         :value-type integer
                         :comparator #'<
                         :eq-pred #'=)

;; Any real number is accepted and stored as a double-float.
(make-class-specific-col :class column
                         :type float
                         :value-type real
                         :comparator #'<
                         :eq-pred #'=
                         :val-norm #'to-double)

;; Universal time: whole seconds since 1900-01-01 UTC.
(make-class-specific-col :class column
                         :type timestamp
                         :value-type (integer 0)
                         :comparator #'<
                         :eq-pred #'=)

(make-class-specific-col :class column
                         :type keyword
                         :value-type keyword
                         :comparator #'string<
                         :eq-pred #'eq)

;; Any Lisp object. Values are compared with EQUAL and can't be sorted.
(make-class-specific-col :class column
                         :type any)

(make-class-specific-col :class interned-values-column
                         :type interned-string
                         :value-type string
                         :comparator #'string<)

(defun make-schema (spec)
  (mapcar #'(lambda (col-spec) (apply #'make-col col-spec)) spec))

(defun validate-schema (table-name schema)
  (let ((names (mapcar #'name schema)))
    (unless (= (length names) (length (remove-duplicates names)))
      (error 'schema-error
             :message (format nil "Duplicate column names in table ~A: ~A" table-name names)))
    (when (> (count-if (lambda (c) (typep c 'primary-key-column)) schema) 1)
      (error 'schema-error
             :message (format nil "Table ~A has more than one primary key column" table-name)))))

(defun make-table (name schema)
  (let ((schema (if (every (lambda (c) (typep c 'column)) schema)
                    schema
                    (make-schema schema))))
    (validate-schema name schema)
    (with-transaction
      (add-table (create-table-instance name schema)))))

;; Value for COL as stored: type and null checks run, then a foreign key must exist.
(defun resolve-column-value (col val)
  (let ((normalised (normalise-for-col val col)))
    (unless (null-value-p normalised)
      (when (and (check-fn col) (not (funcall (check-fn col) normalised)))
        (error 'check-violation :column (name col)
               :message (format nil "Check failed for column ~A: value ~S" (name col) normalised)))
      (when (typep col 'foreign-key-column)
        (check-foreign-key (references col) normalised)))
    normalised))

;; A unique column may not hold the same non-null value in two rows.
;; ID is the row being written. It does not clash with itself.
(defun check-unique (tbl id row cols)
  (dolist (col cols)
    (let ((val (getf row (name col)))
          (same (equality-predicate col)))
      (unless (null-value-p val)
        (fset:do-map (other-id other (rows tbl))
          (let ((other-val (getf other (name col))))
            (when (and (/= other-id id)
                       (not (null-value-p other-val))
                       (funcall same other-val val))
              (error 'unique-violation :table (name tbl) :column (name col)
                     :message (format nil "Duplicate value ~S in unique column ~A of table ~A"
                                      val (name col) (name tbl))))))))))

(defun unique-columns (schema)
  (remove-if-not #'unique-p schema))

(defun find-column (col-name schema)
  (or (find (as-keyword col-name) schema :key #'name)
      (error 'no-such-column :column col-name
             :message (format nil "No column: ~A in schema: ~A" col-name (mapcar #'name schema)))))

(defun check-known-columns (names-and-values schema)
  (unless (evenp (length names-and-values))
    (error 'invalid-row
           :message (format nil "Malformed row, expected :column value pairs: ~S" names-and-values)))
  (iter (for key in names-and-values by #'cddr)
        (when (typep (find-column key schema) 'primary-key-column)
          (error 'invalid-row
                 :message (format nil "Column ~A is a generated primary key" key)))))

(defun supplied-or-default (names-and-values col)
  (let ((val (getf names-and-values (name col) '%missing)))
    (if (eq val '%missing) (column-default col) val)))

(defun normalise-row (names-and-values schema id)
  (check-known-columns names-and-values schema)
  (iter (for col in schema)
        (collect (name col))
        (collect (if (typep col 'primary-key-column)
                     id
                     (resolve-column-value col (supplied-or-default names-and-values col))))))

;; Add a row, return its id. A primary key column holds the id.
(defun insert-row (name-and-values table)
  (with-transaction
    (let* ((tbl (stored-table table))
           (id (1+ (pk tbl)))
           (row (normalise-row name-and-values (schema tbl) id)))
      (check-unique tbl id row (unique-columns (schema tbl)))
      (put-table (updated-table tbl :pk id :rows (fset:with (rows tbl) id row)))
      id)))

(defun get-row (id table)
  (values (fset:lookup (rows (find-table table)) id)))

;; Change some columns of one row. Rows are replaced, never changed in place.
(defun update-row (id names-and-values table)
  (with-transaction
    (let* ((tbl (stored-table table))
           (old (fset:lookup (rows tbl) id)))
      (unless old
        (error 'no-such-row :table (name tbl) :id id
               :message (format nil "No row ~A in table: ~A" id (name tbl))))
      (check-known-columns names-and-values (schema tbl))
      (let ((new (copy-list old))
            (changed (iter (for key in names-and-values by #'cddr)
                           (collect (find-column key (schema tbl))))))
        (iter (for (key val) on names-and-values by #'cddr)
              (setf (getf new (as-keyword key))
                    (resolve-column-value (find-column key (schema tbl)) val)))
        (check-unique tbl id new (unique-columns changed))
        (put-table (updated-table tbl :rows (fset:with (rows tbl) id new)))
        id))))

;; Return t if a row was removed.
(defun delete-row (id table)
  (with-transaction
    (let ((tbl (stored-table table)))
      (when (nth-value 1 (fset:lookup (rows tbl) id))
        (multiple-value-bind (deletes nullifies) (plan-delete tbl id)
          (apply-delete-plan deletes nullifies))
        t))))

;; All foreign key columns that refer to the table called TABLE-NAME,
;; as a list of (table . column).
(defun referencing-columns (table-name)
  (iter outer (for tbl in (all-tables))
        (iter (for col in (schema tbl))
              (when (and (typep col 'foreign-key-column)
                         (equal (references col) table-name))
                (in outer (collect (cons tbl col)))))))

;; Work out everything a delete of row ID changes, before it changes anything.
;; Return two lists: rows to delete as (table-name . id), and rows whose foreign
;; key column becomes NULL as (table-name id column). Signal foreign-key-violation
;; if a :restrict column still refers to a row that is not deleted.
(defun plan-delete (tbl id)
  (let ((deletes '()) (nullifies '()) (restricts '()))
    (flet ((planned-p (tname id)
             (find-if #'(lambda (d) (and (equal (car d) tname) (eql (cdr d) id))) deletes)))
      (labels ((visit (tname id)
                 (unless (planned-p tname id)
                   (push (cons tname id) deletes)
                   (dolist (ref (referencing-columns tname))
                     (destructuring-bind (child . col) ref
                       (fset:do-map (child-id row (rows child))
                         (when (eql (getf row (name col)) id)
                           (ecase (on-delete col)
                             (:restrict (push (list (name child) child-id col) restricts))
                             (:cascade (visit (name child) child-id))
                             (:set-null (push (list (name child) child-id col) nullifies))))))))))
        (visit (name tbl) id))
      (dolist (r restricts)
        (destructuring-bind (child-name child-id col) r
          (unless (planned-p child-name child-id)
            (error 'foreign-key-violation :table child-name :column (name col)
                   :message (format nil "Can't delete row ~A of table ~A: row ~A of table ~A refers to it"
                                    id (name tbl) child-id child-name)))))
      (values deletes
              (remove-if #'(lambda (n) (planned-p (first n) (second n))) nullifies)))))

(defun apply-delete-plan (deletes nullifies)
  (dolist (n nullifies)
    (destructuring-bind (tname id col) n
      (let* ((tbl (stored-table tname))
             (new (copy-list (fset:lookup (rows tbl) id))))
        (setf (getf new (name col)) :null)
        (put-table (updated-table tbl :rows (fset:with (rows tbl) id new))))))
  (dolist (d deletes)
    (let ((tbl (stored-table (car d))))
      (put-table (updated-table tbl :rows (fset:less (rows tbl) (cdr d)))))))

;; Add a column to a table. Old rows get the column default.
(defun add-column (table col-def)
  (let ((col (apply #'make-col col-def)))
    (when (typep col 'primary-key-column)
      (error 'schema-error
             :message "Can't add a primary key column to an existing table"))
    (with-transaction
      (let ((tbl (stored-table table))
            (new-rows (fset:empty-map))
            (vals '()))
        (when (find (name col) (schema tbl) :key #'name)
          (error 'schema-error
                 :message (format nil "Column ~A already exists in table ~A" (name col) (name tbl))))
        (fset:do-map (id row (rows tbl))
          (let ((val (resolve-column-value col (column-default col))))
            (unless (null-value-p val) (push val vals))
            (setf new-rows (fset:with new-rows id (append row (list (name col) val))))))
        (when (unique-p col)
          (loop for (val . rest) on vals
                when (member val rest :test (equality-predicate col))
                  do (error 'unique-violation :table (name tbl) :column (name col)
                            :message (format nil "Duplicate value ~S in unique column ~A of table ~A"
                                             val (name col) (name tbl)))))
        (put-table (updated-table tbl :rows new-rows
                                      :schema (append (schema tbl) (list col))))
        col))))

;; Rows of a table as a list of plists, in id order.
(defun row-list (table)
  (rows-list (rows (find-table table))))

;; Run a query. WHERE is a function of one row, see MATCHING.
;; INNER-JOIN is (table-name column-here column-there).
;; Return a new table. Its rows are keyed 1..n in result order.
(defun select (&key (col t) from where distinct order-by inner-join)
  (with-snapshot
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
      (make-result-table rows schema))))

(defun make-result-table (rows schema)
  (make-instance 'table
                 :schema schema
                 :name (gensym "SELECT")
                 :pk (length rows)
                 :rows (fset:convert 'fset:map
                                     (loop for row in rows
                                           for id from 1
                                           collect (cons id row)))))

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
        (error 'query-error
               :message (format nil "Can't join: both tables have columns named ~A" clash)))
      (values (loop for a in rows
                    nconc (loop for b in other-rows
                                when (let ((x (getf a (name here-col)))
                                           (y (getf b (name there-col))))
                                       (and (not (null-value-p x))
                                            (not (null-value-p y))
                                            (funcall (equality-predicate here-col) x y)))
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
              (always (let ((x (getf a name))
                            (y (getf b name)))
                        ;; two NULLs count as equal here, so DISTINCT keeps one
                        (cond ((and (null-value-p x) (null-value-p y)) t)
                              ((or (null-value-p x) (null-value-p y)) nil)
                              (t (funcall test x y)))))))))

;; True when row A sorts before row B. The first column that differs decides.
;; NULL sorts after every other value.
(defun row-comparator (col-names schema)
  (let* ((names (mapcar #'as-keyword col-names))
         (comparators (mapcar #'(lambda (col)
                                  (or (comparator col)
                                      (error 'query-error
                                             :message (format nil "Column ~A has no comparator" (name col)))))
                              (extract-schema names schema))))
    #'(lambda (a b)
        (loop for name in names
              for comp in comparators
              do (let ((a-v (getf a name))
                       (b-v (getf b name)))
                   (cond ((and (null-value-p a-v) (null-value-p b-v)))
                         ((null-value-p a-v) (return nil))
                         ((null-value-p b-v) (return t))
                         ((funcall comp a-v b-v) (return t))
                         ((funcall comp b-v a-v) (return nil))))
              finally (return nil)))))

(defun column-matcher (col val)
  (let ((name (name col))
        (predicate (equality-predicate col)))
    (if (null-value-p val)
        #'(lambda (row) (null-value-p (getf row name)))
        (let ((normalised (normalise-for-col val col)))
          #'(lambda (row)
              (let ((row-val (getf row name)))
                ;; a NULL column value never matches a given value
                (and (not (null-value-p row-val))
                     (funcall predicate row-val normalised))))))))

;; A nil value means "any value". The value :null matches rows where the column is NULL.
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

(defparameter +file-format-version+ 2)

(defun table-snapshot (tbl)
  (list (name tbl)
        (pk tbl)
        (mapcar #'spec (schema tbl))
        (fset:convert 'list (rows tbl))))

(defun restore-table (snapshot)
  (destructuring-bind (name pk col-specs id-rows) snapshot
    (make-instance 'table :name name :pk pk
                          :schema (make-schema col-specs)
                          :rows (fset:convert 'fset:map id-rows))))

;; A file holds data only. A function option in a column spec (:check or
;; :default-fn) is saved if it is a function name, but not if it is a function object.
(defun check-savable (tables)
  (iter (for tbl in tables)
        (iter (for col in (schema tbl))
              (iter (for (key val) on (cddr (spec col)) by #'cddr)
                    (when (functionp val)
                      (error 'schema-error
                             :message (format nil "Can't save column ~A of table ~A: option ~S is a function object. Use a function name."
                                              (name col) (name tbl) key)))))))

;; Write all committed tables to FILE. Return FILE.
;; Changes of an open transaction are not saved until it commits.
;; The save reads one fixed root, so writers do not wait for it.
(defun save-db (file)
  (get-lock *file-lock*
    (let ((tables (all-tables (db-root *db*))))
      (check-savable tables)
      (cl-store:store (list :hash-db +file-format-version+ (mapcar #'table-snapshot tables))
                      file)
      file)))

;; Replace all tables with those in FILE. Return the table names.
(defun load-db (file)
  (when *txn*
    (error 'transaction-error :message "Can't load a database file inside a transaction"))
  (get-lock *file-lock*
    (let ((data (cl-store:restore file)))
      (unless (and (consp data)
                   (eq (first data) :hash-db)
                   (eql (second data) +file-format-version+))
        (error 'file-format-error :file file
               :message (format nil "~A is not a hash-db file" file)))
      (let ((root (fset:empty-map)))
        (dolist (tbl (mapcar #'restore-table (third data)))
          (setf root (fset:with root (name tbl) tbl)))
        (with-writer
          (setf (db-root *db*) root))
        (show-tables)))))
