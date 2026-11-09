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
  root
  ;; number of the last committed transaction that changed data
  (seq 0)
  ;; the write-ahead log of an open database, else nil. See wal.lisp.
  (wal nil)
  ;; function of (seq entries) that writes a commit to the log
  (commit-fn nil)
  ;; function of no arguments, called after a commit is visible
  (post-commit-fn nil)
  ;; the background thread that removes expired rows, or nil. See ttl.lisp.
  (sweeper nil)
  ;; Kept older roots, newest first, as (seq time root). See history.lisp.
  (history nil)
  ;; how many to keep. 0 keeps none.
  (history-limit 0)
  ;; the snapshot file of an open database
  (path nil))

(defvar *db* (make-db))

;; State of the open transaction in this thread. It holds the working root and
;; the journal: what the transaction changed, newest entry first.
;; Entries are plain data: (:create-table name specs), (:drop-table name),
;; (:insert table id row), (:update table id row), (:delete table id) and
;; (:add-column table spec id-rows), (:create-index table name column unique)
;; and (:drop-index table name). REPLAY-ENTRIES applies them to a root.
(defstruct (txn (:constructor make-txn (root)))
  root
  (journal nil))

;; Called with (seq entries) just before a transaction commits. If it signals
;; an error, the transaction does not commit.
(defvar *commit-hook* nil)

(defvar *txn* nil)

;; True inside WITH-AS-OF. The database can't be changed there.
(defvar *as-of* nil)

;; The time. Universal time, in whole seconds. Bind or set it to test anything that
;; depends on time.
(defvar *clock* #'get-universal-time)

(defun now ()
  (funcall *clock*))

(defun journal-add (entry)
  (push entry (txn-journal *txn*)))

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
        (when *as-of*
          (error 'transaction-error
                 :message "The database is read only inside with-as-of"))
        (let ((*txn* (make-txn (db-root *db*))))
          (catch 'rollback
            (multiple-value-prog1 (funcall fn)
              (commit-transaction)))))))

;; Make the working root the committed root. A transaction that changed
;; data gets the next number and is passed to the commit hook first.
(defun commit-transaction ()
  (let ((entries (reverse (txn-journal *txn*))))
    (when entries
      (let ((seq (1+ (db-seq *db*))))
        (when *commit-hook*
          (funcall *commit-hook* seq entries))
        (when (db-commit-fn *db*)
          (funcall (db-commit-fn *db*) seq entries))
        (setf (db-seq *db*) seq)))
    (setf (db-root *db*) (txn-root *txn*))
    (when (and entries (plusp (db-history-limit *db*)))
      (record-history (db-seq *db*) (db-root *db*)))
    (when (and entries (db-post-commit-fn *db*))
      (funcall (db-post-commit-fn *db*)))))

;; Keep the newest committed roots, as many as the limit says.
(defun record-history (seq root)
  (let ((limit (db-history-limit *db*)))
    (push (list seq (now) root) (db-history *db*))
    (when (> (length (db-history *db*)) limit)
      (setf (db-history *db*) (subseq (db-history *db*) 0 limit)))))

(defun call-with-savepoint (fn)
  (let ((saved (txn-root *txn*))
        (saved-journal (txn-journal *txn*))
        (done nil))
    (unwind-protect
         (catch 'rollback
           (multiple-value-prog1 (funcall fn)
             (setf done t)))
      (unless done
        (setf (txn-root *txn*) saved
              (txn-journal *txn*) saved-journal)))))

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
                  :initform        nil)
   ;; list of INDEX structures. See the section on indexes.
   (indexes       :reader indexes
                  :initarg         :indexes
                  :initform        nil)
   ;; list of TRIGGER structures. See the section on triggers.
   (triggers      :reader triggers
                  :initarg         :triggers
                  :initform        nil)
   ;; the column that holds when a row expires, or nil. See ttl.lisp.
   (ttl           :reader ttl
                  :initarg         :ttl
                  :initform        nil))
  (:documentation "A table in a database."))

(defmethod get-schema ((table table))
  (schema table))

(defun create-table-instance (name schema)
  (ensure-implicit-indexes (make-instance 'table :name name :schema schema)))

(defun updated-table (tbl &key (rows (rows tbl)) (pk (pk tbl)) (schema (schema tbl))
                            (indexes (indexes tbl)) (triggers (triggers tbl)) (ttl (ttl tbl)))
  (make-instance 'table :name (name tbl) :rows rows :pk pk :schema schema :indexes indexes
                        :triggers triggers :ttl ttl))

;; Store or remove any named thing in the working root. Only inside WITH-TRANSACTION.
(defun put-object (name object)
  (setf (txn-root *txn*) (fset:with (txn-root *txn*) name object))
  object)

(defun remove-object (name)
  (setf (txn-root *txn*) (fset:less (txn-root *txn*) name)))

;; Any named thing in the working root, or nil.
(defun named-object (name)
  (values (fset:lookup (current-root) name)))

;; Signal schema-error if NAME is taken by a table or by anything else.
(defun check-name-free (name what)
  (when (nth-value 1 (fset:lookup (current-root) name))
    (error 'schema-error
           :message (format nil "Can't make ~A ~A: the name is already used" what name))))

;; Store TBL in the working root. Only call this inside WITH-TRANSACTION.
(defun put-table (tbl)
  (setf (txn-root *txn*) (fset:with (txn-root *txn*) (name tbl) tbl))
  tbl)

(defun rows-list (rows)
  (mapcar #'cdr (fset:convert 'list rows)))

;; The root holds tables, and other named things that features add, such as
;; sequences and views. They share one namespace. These functions see tables only.
(defun all-tables (&optional (root (current-root)))
  (loop for name in (fset:convert 'list (fset:domain root))
        for object = (values (fset:lookup root name))
        when (typep object 'table) collect object))

(defun show-tables ()
  (mapcar #'name (all-tables)))

;; Return a table of NAME, or nil
(defun get-named-table (name)
  (let ((object (values (fset:lookup (current-root) name))))
    (and (typep object 'table) object)))

;; A view is a stored query. See views.lisp.
(defstruct (view (:constructor make-view (name query)))
  name
  query)

(declaim (ftype function run-view))

;; Accept a table name, a table value (for example a select result), or the name
;; of a view. A view is run, and gives a table value.
(defun find-table (designator)
  (cond ((typep designator 'table) designator)
        ((get-named-table designator))
        ((typep (named-object designator) 'view) (run-view (named-object designator)))
        (t (error 'no-such-table :table designator
                  :message (format nil "The table: ~A does not exist" designator)))))

;; The stored table for a name or table value. Changes need a stored table.
(defun stored-table (designator)
  (let ((name (if (typep designator 'table) (name designator) designator)))
    (when (typep (named-object name) 'view)
      (error 'schema-error
             :message (format nil "~A is a view. Change the tables under it." name)))
    (or (get-named-table name)
        (error 'no-such-table :table name
               :message (format nil "The table: ~A does not exist" name)))))

;; Add a table to the database. A duplicate name is an error.
(defun add-table (tbl)
  (when (nth-value 1 (fset:lookup (txn-root *txn*) (name tbl)))
    (error 'table-exists :table (name tbl)
           :message (format nil "The table: ~A already exists" (name tbl))))
  (journal-add (list :create-table (name tbl) (mapcar #'spec (schema tbl))))
  (put-table tbl))

(defun drop-table (name)
  (with-transaction
    (when (typep (named-object name) 'view)
      (error 'schema-error :message (format nil "~A is a view. Use drop-view." name)))
    (when (get-named-table name)
      (let ((refs (remove name (referencing-columns name)
                          :key #'(lambda (ref) (name (car ref))) :test #'equal)))
        (when refs
          (error 'schema-error
                 :message (format nil "Can't drop table ~A: table ~A refers to it"
                                  name (name (car (first refs)))))))
      (setf (txn-root *txn*) (fset:less (txn-root *txn*) name))
      (journal-add (list :drop-table name))
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
   ;; Interning saves memory. Equality must not depend on it, because
   ;; rows loaded from a file hold strings that were never interned.
   (equality-predicate :initform #'string=)
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

;;; Indexes
;;;
;;; An index finds rows by the value of one column without a scan. It is an
;;; immutable fset map from a normalised value to the ids of the rows that hold it.
;;; Rows with NULL in the column are not in the index. A table holds its indexes,
;;; and every change to a row makes new indexes with it, so a snapshot of the
;;; database always has indexes that match its rows.
;;;
;;; Some indexes are implicit. A :unique column and a foreign key column get one,
;;; so the checks on them do not scan. They come from the schema and are rebuilt
;;; when a file loads. The primary key needs none, because rows are stored by id.

;; Values of one kind must give keys that compare as the column compares them.
;; Numbers become rationals, so 1 and 1.0 are one key. Strings are wrapped, because
;; fset orders plain strings by length first, and a range scan needs string< order.
(defstruct (skey (:constructor skey (string)))
  string)

(defmethod fset:compare ((a skey) (b skey))
  (let ((x (skey-string a))
        (y (skey-string b)))
    (cond ((string< x y) :less)
          ((string> x y) :greater)
          (t :equal))))

(defun index-key (value)
  (cond ((realp value) (rational value))
        ((stringp value) (skey value))
        (t value)))

;; Equal values must give equal keys in a hash table too. Numbers match by value,
;; as = does.
(defun distinct-key (value)
  (if (realp value) (rational value) value))

;; MAP holds key -> id for a unique index, and key -> fset set of ids for any other.
(defstruct (index (:constructor make-index (name column unique explicit
                                            &optional (map (fset:empty-map)))))
  name
  column
  unique
  explicit
  map)

(defun index-add (index id value)
  (if (null-value-p value)
      index
      (let* ((key (index-key value))
             (map (index-map index))
             (new (copy-index index)))
        (setf (index-map new)
              (fset:with map key
                         (if (index-unique index)
                             id
                             (fset:with (or (fset:lookup map key) (fset:empty-set)) id))))
        new)))

(defun index-remove (index id value)
  (if (null-value-p value)
      index
      (let* ((key (index-key value))
             (map (index-map index))
             (entry (fset:lookup map key))
             (new (copy-index index)))
        (setf (index-map new)
              (cond ((null entry) map)
                    ((index-unique index) (if (eql entry id) (fset:less map key) map))
                    (t (let ((rest (fset:less entry id)))
                         (if (zerop (fset:size rest))
                             (fset:less map key)
                             (fset:with map key rest))))))
        new)))

;; The ids of rows whose column equals VALUE, in id order.
(defun index-ids (index value)
  (let ((entry (and (not (null-value-p value))
                    (fset:lookup (index-map index) (index-key value)))))
    (cond ((null entry) nil)
          ((index-unique index) (list entry))
          (t (fset:convert 'list entry)))))

;; How many keys of MAP sort before KEY, and how many sort before or equal to it.
;; FSET:RANK gives the rank of KEY when it is there. When it is not, it gives the rank
;; of the key just below it, or -1.
(defun keys-before (map key)
  (multiple-value-bind (rank found) (fset:rank map key)
    (if found rank (1+ rank))))

(defun keys-through (map key)
  (1+ (fset:rank map key)))

;; The ids of rows whose value lies between LOW and HIGH, in value order. Pass
;; nil for no limit on that side. Only for columns whose values are all real
;; numbers or all strings.
(defun index-range-ids (index low low-inclusive high high-inclusive)
  (let* ((map (index-map index))
         (start (cond ((null low) 0)
                      (low-inclusive (keys-before map (index-key low)))
                      (t (keys-through map (index-key low)))))
         (end (cond ((null high) (fset:size map))
                    (high-inclusive (keys-through map (index-key high)))
                    (t (keys-before map (index-key high)))))
         (ids '()))
    (loop for rank from start below end
          do (let ((entry (nth-value 1 (fset:at-rank map rank))))
               (if (index-unique index)
                   (push entry ids)
                   (fset:do-set (id entry) (push id ids)))))
    (nreverse ids)))

;; A range scan needs an order that matches fset's order for the keys.
(defun range-column-p (column)
  (let ((type (value-type column)))
    (and (not (eq type t))
         (or (subtypep type 'real) (subtypep type 'string)))))

(defun table-index-for (tbl column &optional unique-only)
  "An index of TBL on COLUMN. A unique index comes first."
  (let ((candidates (remove-if-not #'(lambda (ix) (eq (index-column ix) column)) (indexes tbl))))
    (or (find-if #'index-unique candidates)
        (and (not unique-only) (first candidates)))))

(defun build-index (tbl name column unique explicit)
  (let ((index (make-index name column unique explicit)))
    (fset:do-map (id row (rows tbl))
      (let ((value (getf row column)))
        (when (and unique
                   (not (null-value-p value))
                   (fset:lookup (index-map index) (index-key value)))
          (error 'unique-violation :table (name tbl) :column column
                 :message (format nil "Duplicate value ~S in unique column ~A of table ~A"
                                  value column (name tbl))))
        (setf index (index-add index id value))))
    index))

(defun implicit-index-specs (schema)
  "A list of (name column unique) for the columns that need an index."
  (loop for col in schema
        when (unique-p col)
          collect (list (as-keyword (format nil "~A-UNIQUE" (name col))) (name col) t)
        else when (typep col 'foreign-key-column)
          collect (list (as-keyword (format nil "~A-FK" (name col))) (name col) nil)))

;; Add the implicit indexes that TBL does not have yet.
(defun ensure-implicit-indexes (tbl)
  (let ((indexes (indexes tbl)))
    (dolist (spec (implicit-index-specs (schema tbl)))
      (destructuring-bind (name column unique) spec
        (unless (find name indexes :key #'index-name)
          (setf indexes (append indexes (list (build-index tbl name column unique nil)))))))
    (if (eq indexes (indexes tbl))
        tbl
        (updated-table tbl :indexes indexes))))

;;; Making and dropping indexes. A table has the implicit indexes of its schema, and
;;; the indexes that were made here. Only these are saved and logged.

(defun index-named (tbl name)
  (find name (indexes tbl) :key #'index-name))

;; Make an index on COLUMN of TABLE and return its name. The default name is
;; :COLUMN-INDEX. With :unique t, two rows may not hold the same non-null value,
;; and the call fails if they already do.
(defun create-index (table column &key name unique)
  (with-transaction
    (let* ((tbl (stored-table table))
           (col (find-column column (schema tbl)))
           (unique (and unique t))
           (name (as-keyword (or name (format nil "~A-INDEX" (name col))))))
      (when (typep col 'primary-key-column)
        (error 'schema-error
               :message (format nil "Column ~A is the primary key. Rows are stored by it already."
                                (name col))))
      (when (index-named tbl name)
        (error 'schema-error
               :message (format nil "Table ~A already has an index named ~A" (name tbl) name)))
      (let ((twin (find-if #'(lambda (ix) (and (eq (index-column ix) (name col))
                                               (eq (and (index-unique ix) t) unique)))
                           (indexes tbl))))
        (when twin
          (error 'schema-error
                 :message (format nil "Column ~A of table ~A already has the ~:[~;unique ~]index ~A"
                                  (name col) (name tbl) unique (index-name twin)))))
      (let ((index (build-index tbl name (name col) unique t)))
        (put-table (updated-table tbl :indexes (append (indexes tbl) (list index))))
        (journal-add (list :create-index (name tbl) name (name col) unique))
        name))))

;; Drop an index that CREATE-INDEX made. Return t, or nil if there is no such index.
;; An index that comes from the schema can't be dropped.
(defun drop-index (name table)
  (with-transaction
    (let* ((tbl (stored-table table))
           (name (as-keyword name))
           (index (index-named tbl name)))
      (when index
        (unless (index-explicit index)
          (error 'schema-error
                 :message (format nil "Index ~A comes from the schema of table ~A. Change the column to drop it."
                                  name (name tbl))))
        (put-table (updated-table tbl :indexes (remove index (indexes tbl))))
        (journal-add (list :drop-index (name tbl) name))
        t))))

;; The indexes of a table as a list of (:name n :column c :unique u :explicit e).
(defun show-indexes (table)
  (mapcar #'(lambda (ix)
              (list :name (index-name ix) :column (index-column ix)
                    :unique (and (index-unique ix) t) :explicit (and (index-explicit ix) t)))
          (indexes (find-table table))))

;; The table with row ID set to ROW, with its indexes kept in step. Use this and
;; TABLE-WITHOUT-ROW for every change to the rows of a table.
(defun table-with-row (tbl id row &key (pk (pk tbl)))
  (let ((old (fset:lookup (rows tbl) id)))
    (updated-table
     tbl
     :rows (fset:with (rows tbl) id row)
     :pk pk
     :indexes (mapcar #'(lambda (index)
                          (let* ((column (index-column index))
                                 (old-value (if old (getf old column :null) :null))
                                 (new-value (getf row column :null)))
                            (if (and (not (null-value-p old-value))
                                     (not (null-value-p new-value))
                                     (eq :equal (fset:compare (index-key old-value)
                                                              (index-key new-value))))
                                index
                                (index-add (index-remove index id old-value) id new-value))))
                      (indexes tbl)))))

(defun table-without-row (tbl id)
  (let ((old (fset:lookup (rows tbl) id)))
    (updated-table
     tbl
     :rows (fset:less (rows tbl) id)
     :indexes (mapcar #'(lambda (index)
                          (index-remove index id (if old (getf old (index-column index) :null) :null)))
                      (indexes tbl)))))

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
(defun check-unique (tbl id row)
  (dolist (index (indexes tbl))
    (when (index-unique index)
      (let ((value (getf row (index-column index))))
        (when (find-if #'(lambda (other) (/= other id)) (index-ids index value))
          (error 'unique-violation :table (name tbl) :column (index-column index)
                 :message (format nil "Duplicate value ~S in unique column ~A of table ~A"
                                  value (index-column index) (name tbl))))))))

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
;;; Triggers
;;;
;;; A trigger is a function that runs when a row of a table is inserted, updated or
;;; deleted. See triggers.lisp for how to make one. The function takes four
;;; arguments: the event (:insert, :update or :delete), the table name, the old row
;;; (nil for an insert) and the new row (nil for a delete).
;;;
;;; A :before trigger runs before the change is made. It can return a list of
;;; :column value pairs. These change the new row, and the usual checks run on them.
;;; It can signal an error to stop the change. It may change other tables, but
;;; not its own table: that is a trigger-error. An :after trigger runs after the
;;; change and may do anything. Triggers run inside the transaction of the change.
;;; If one signals an error, the whole change is dropped. A change made by a cascade
;;; fires triggers too. Replaying the log does not, because what the triggers did
;;; was logged with the change.

(defstruct (trigger (:constructor make-trigger (name timing events function)))
  name
  timing
  events
  function)

(defparameter *max-trigger-depth* 32)
(defvar *trigger-depth* 0)

(defun matching-triggers (tbl timing event)
  (remove-if-not #'(lambda (trigger)
                     (and (eq (trigger-timing trigger) timing)
                          (member event (trigger-events trigger))))
                 (triggers tbl)))

(defun call-trigger (trigger tbl event old new)
  (let ((*trigger-depth* (1+ *trigger-depth*)))
    (when (> *trigger-depth* *max-trigger-depth*)
      (error 'trigger-error
             :message (format nil "Trigger ~A on table ~A runs more than ~D levels deep. Does it call itself?"
                              (trigger-name trigger) (name tbl) *max-trigger-depth*)))
    (funcall (trigger-function trigger) event (name tbl) old new)))

;; The new row with changes from a list of :column value pairs. The checks of the
;; schema run on the changed columns. The primary key can't be changed.
(defun row-with-changes (tbl row changes)
  (check-known-columns changes (schema tbl))
  (let ((new (copy-list row)))
    (iter (for (key val) on changes by #'cddr)
          (setf (getf new (as-keyword key))
                (resolve-column-value (find-column key (schema tbl)) val)))
    new))

;; Run the :before triggers. Return the new row with their changes.
(defun fire-before (tbl event old new)
  (let ((triggers (matching-triggers tbl :before event)))
    (dolist (trigger triggers)
      (let ((changes (call-trigger trigger tbl event old new)))
        (when (and changes new)
          (setf new (row-with-changes tbl new changes)))))
    (when (and triggers (not (eq (stored-table (name tbl)) tbl)))
      (error 'trigger-error
             :message (format nil "A :before trigger changed its own table ~A. Use an :after trigger for that."
                              (name tbl))))
    new))

(defun fire-after (tbl event old new)
  (dolist (trigger (matching-triggers tbl :after event))
    (call-trigger trigger tbl event old new)))

(defun insert-row (name-and-values table)
  (with-transaction
    (let* ((tbl (stored-table table))
           (id (1+ (pk tbl)))
           (row (fire-before tbl :insert nil (normalise-row name-and-values (schema tbl) id))))
      (check-unique tbl id row)
      (put-table (table-with-row tbl id row :pk id))
      (journal-add (list :insert (name tbl) id row))
      (fire-after tbl :insert nil row)
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
      (let ((new (fire-before tbl :update old (row-with-changes tbl old names-and-values))))
        (check-unique tbl id new)
        (put-table (table-with-row tbl id new))
        (journal-add (list :update (name tbl) id new))
        (fire-after tbl :update old new)
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
                       (dolist (child-id (index-ids (table-index-for child (name col)) id))
                         (ecase (on-delete col)
                           (:restrict (push (list (name child) child-id col) restricts))
                           (:cascade (visit (name child) child-id))
                           (:set-null (push (list (name child) child-id col) nullifies)))))))))
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
             (old (fset:lookup (rows tbl) id)))
        ;; a trigger may have deleted the row already
        (when old
          (let ((new (fire-before tbl :update old
                                  (let ((row (copy-list old)))
                                    (setf (getf row (name col)) :null)
                                    row))))
            (put-table (table-with-row tbl id new))
            (journal-add (list :update tname id new))
            (fire-after tbl :update old new))))))
  (dolist (d deletes)
    (let* ((tbl (stored-table (car d)))
           (old (fset:lookup (rows tbl) (cdr d))))
      (when old
        (fire-before tbl :delete old nil)
        (put-table (table-without-row tbl (cdr d)))
        (journal-add (list :delete (car d) (cdr d)))
        (fire-after tbl :delete old nil)))))

;; Add a column to a table. Old rows get the column default.
(defun add-column (table col-def)
  (let ((col (apply #'make-col col-def)))
    (when (typep col 'primary-key-column)
      (error 'schema-error
             :message "Can't add a primary key column to an existing table"))
    (with-transaction
      (let ((tbl (stored-table table))
            (new-rows (fset:empty-map)))
        (when (find (name col) (schema tbl) :key #'name)
          (error 'schema-error
                 :message (format nil "Column ~A already exists in table ~A" (name col) (name tbl))))
        (fset:do-map (id row (rows tbl))
          (let ((val (resolve-column-value col (column-default col))))
            (setf new-rows (fset:with new-rows id (append row (list (name col) val))))))
        ;; the implicit index of the new column is built here, and a unique
        ;; column with a repeated default fails here
        (put-table (ensure-implicit-indexes
                    (updated-table tbl :rows new-rows
                                       :schema (append (schema tbl) (list col)))))
        (journal-add (list :add-column (name tbl) (spec col) (fset:convert 'list new-rows)))
        col))))

;; Rows of a table as a list of plists, in id order.
(defun row-list (table)
  (rows-list (rows (find-table table))))

;;; Persistence. A file holds plain data: table names, row ids, rows and
;;; column specs. Locks and functions are rebuilt on load.

(defparameter +file-format-version+ 5)

;; Files of these versions can be read. Version 4 added the indexes that
;; CREATE-INDEX made. Version 5 added sections for features. An older file has none.
(defparameter +readable-file-versions+ '(3 4 5))

(defun table-snapshot (tbl)
  (list (name tbl)
        (pk tbl)
        (mapcar #'spec (schema tbl))
        (fset:convert 'list (rows tbl))
        ;; only the indexes that CREATE-INDEX made. The others come from the schema.
        (loop for ix in (indexes tbl)
              when (index-explicit ix)
                collect (list (index-name ix) (index-column ix) (and (index-unique ix) t)))
        (loop for trigger in (triggers tbl)
              collect (list (trigger-name trigger) (trigger-timing trigger)
                            (trigger-events trigger) (trigger-function trigger)))
        (ttl tbl)))

(defun restore-table (snapshot)
  (destructuring-bind (name pk col-specs id-rows &optional index-specs trigger-specs ttl) snapshot
    (let ((tbl (ensure-implicit-indexes
                (make-instance 'table :name name :pk pk
                                      :schema (make-schema col-specs)
                                      :rows (fset:convert 'fset:map id-rows)
                                      :triggers (mapcar #'(lambda (spec) (apply #'make-trigger spec))
                                                        trigger-specs)
                                      :ttl ttl))))
      (dolist (spec index-specs tbl)
        (destructuring-bind (index-name column unique) spec
          (setf tbl (updated-table tbl :indexes (append (indexes tbl)
                                                        (list (build-index tbl index-name column
                                                                           unique t))))))))))

;; A file holds data only. A function option in a column spec (:check or
;; :default-fn) is saved if it is a function name, but not if it is a function object.
;; A value that holds a function or a stream can't be restored either.
(defun unsavable-p (obj)
  (typecase obj
    ((or function stream) t)
    (cons (or (unsavable-p (car obj)) (unsavable-p (cdr obj))))
    ((and vector (not string)) (some #'unsavable-p obj))
    (t nil)))

(defun check-savable (tables)
  (iter (for tbl in tables)
        (iter (for col in (schema tbl))
              (iter (for (key val) on (cddr (spec col)) by #'cddr)
                    (when (functionp val)
                      (error 'schema-error
                             :message (format nil "Can't save column ~A of table ~A: option ~S is a function object. Use a function name."
                                              (name col) (name tbl) key)))))
        (fset:do-map (id row (rows tbl))
          (when (unsavable-p row)
            (error 'persistence-error
                   :message (format nil "Can't save row ~A of table ~A: it holds a function or a stream"
                                    id (name tbl)))))))

;;; Replay. Apply journal entries to a root and return the new root. Entries
;;; were checked when they first ran, so replay does not check them again.

(defun replay-table-entry (root entry)
  (flet ((table-named (name)
           (or (values (fset:lookup root name))
               (error 'persistence-error
                      :message (format nil "Log entry ~S refers to a missing table" entry)))))
    (ecase (first entry)
      (:create-table
       (destructuring-bind (name specs) (rest entry)
         (fset:with root name (create-table-instance name (make-schema specs)))))
      (:drop-table
       (fset:less root (second entry)))
      (:insert
       (destructuring-bind (name id row) (rest entry)
         (let ((tbl (table-named name)))
           (fset:with root name (table-with-row tbl id row :pk id)))))
      (:update
       (destructuring-bind (name id row) (rest entry)
         (let ((tbl (table-named name)))
           (fset:with root name (table-with-row tbl id row)))))
      (:delete
       (destructuring-bind (name id) (rest entry)
         (let ((tbl (table-named name)))
           (fset:with root name (table-without-row tbl id)))))
      (:create-index
       (destructuring-bind (name index-name column unique) (rest entry)
         (let ((tbl (table-named name)))
           (fset:with root name
                      (updated-table tbl :indexes (append (indexes tbl)
                                                          (list (build-index tbl index-name column unique t))))))))
      (:drop-index
       (destructuring-bind (name index-name) (rest entry)
         (let ((tbl (table-named name)))
           (fset:with root name
                      (updated-table tbl :indexes (remove index-name (indexes tbl)
                                                          :key #'index-name))))))
      (:add-column
       (destructuring-bind (name spec id-rows) (rest entry)
         (let ((tbl (table-named name)))
           (fset:with root name
                      (ensure-implicit-indexes
                       (updated-table tbl
                                      :schema (append (schema tbl) (list (apply #'make-col spec)))
                                      :rows (fset:convert 'fset:map id-rows))))))))))

;;; Extending replay and snapshots
;;;
;;; A feature that adds named things to the root registers three things here: how
;;; to replay its log entries, how to save its things in a snapshot and restore them,
;;; and what must be true before a save.

(defvar *replay-handlers* (make-hash-table :test 'eq))

;; Define how to replay an entry (kind . args). BODY returns the new root.
(defmacro define-replay (kind (root &rest args) &body body)
  `(setf (gethash ,kind *replay-handlers*)
         (lambda (,root entry)
           (declare (ignorable entry))
           (destructuring-bind ,args (rest entry) ,@body))))

(defun replay-entry (root entry)
  (let ((handler (gethash (first entry) *replay-handlers*)))
    (cond (handler (funcall handler root entry))
          ((member (first entry) '(:create-table :drop-table :insert :update :delete
                                   :create-index :drop-index :add-column))
           (replay-table-entry root entry))
          (t (error 'persistence-error
                    :message (format nil "Unknown log entry ~S. The log may come from a newer version."
                                     (first entry)))))))

(defun replay-entries (root entries)
  (dolist (entry entries root)
    (setf root (replay-entry root entry))))

;;; Safe file writes. Data goes to a temporary file first. The temporary file is
;;; flushed to disk, then renamed over the target. A crash leaves either the
;;; old file or the new file, never a mix. Flushing needs SBCL. Other
;;; implementations rename without flushing.

(defun native-path (path)
  (uiop:native-namestring (merge-pathnames path)))

(defun fsync-path (path)
  #+sbcl
  (let ((fd (sb-posix:open (native-path path) sb-posix:o-rdonly)))
    (unwind-protect (sb-posix:fsync fd)
      (sb-posix:close fd)))
  #-sbcl
  (declare (ignore path)))

;; Make a rename or a new file durable. The change lives in the directory.
(defun fsync-directory (path)
  (fsync-path (uiop:pathname-directory-pathname (merge-pathnames path))))

(defun rename-over (from to)
  #+sbcl (sb-posix:rename (native-path from) (native-path to))
  #-sbcl (uiop:rename-file-overwriting-target from to))

;; Call WRITER with the name of a temporary file. When it returns, move the
;; temporary file to FILE. If it fails, delete the temporary file and leave
;; FILE as it was.
(defun call-with-atomic-file (file writer)
  (let* ((target (merge-pathnames file))
         (tmp (uiop:parse-native-namestring (concatenate 'string (native-path target) ".tmp")))
         (done nil))
    (unwind-protect
         (progn (funcall writer tmp)
                (fsync-path tmp)
                (rename-over tmp target)
                (fsync-directory target)
                (setf done t))
      (unless done
        (uiop:delete-file-if-exists tmp)))
    target))

;; The committed root and its transaction number, read together.
(defun committed-state ()
  (with-writer
    (values (db-root *db*) (db-seq *db*))))

;; Write the tables of ROOT to FILE as the state after transaction SEQ.
(defvar *snapshot-sections* '())

;; Save and restore the things of one feature. SAVER takes the root and returns data.
;; RESTORER takes a root and the data and returns the new root. Sections run in the order
;; they were registered.
(defun register-snapshot-section (key saver restorer)
  (let ((existing (assoc key *snapshot-sections*)))
    (if existing
        (setf (cdr existing) (list saver restorer))
        (setf *snapshot-sections* (append *snapshot-sections* (list (list key saver restorer)))))))

(defvar *save-checks* '())

;; CHECK takes the root and signals persistence-error if it holds something a file can't keep.
(defun register-save-check (check)
  (pushnew check *save-checks*))

(defun write-snapshot (file root seq)
  (let ((tables (all-tables root)))
    (check-savable tables)
    (dolist (check *save-checks*)
      (funcall check root))
    (call-with-atomic-file
     file
     #'(lambda (tmp)
         (cl-store:store (list :hash-db +file-format-version+ seq (mapcar #'table-snapshot tables)
                               (loop for (key saver) in *snapshot-sections*
                                     append (list key (funcall saver root))))
                         tmp)))
    file))

;; Read a snapshot file. Return the root and the transaction number.
(defun read-snapshot (file)
  (flet ((bad (why)
           (error 'file-format-error :file file
                  :message (format nil "~A is not a hash-db file~@[ (~A)~]" file why))))
    (let ((data (handler-case (cl-store:restore file)
                  ((or cl-store:restore-error end-of-file) (c) (bad (princ-to-string c))))))
      (unless (and (consp data)
                   (eq (first data) :hash-db)
                   (member (second data) +readable-file-versions+)
                   (integerp (third data))
                   (listp (fourth data)))
        (bad nil))
      (let ((root (fset:empty-map)))
        (dolist (tbl (mapcar #'restore-table (fourth data)))
          (setf root (fset:with root (name tbl) tbl)))
        (loop for (key nil restorer) in *snapshot-sections*
              for sections = (fifth data)
              when (member key sections)
                do (setf root (funcall restorer root (getf sections key))))
        (values root (third data))))))

;; Write all committed tables to FILE. Return FILE.
;; Changes of an open transaction are not saved until it commits.
(defun save-db (file)
  (when (and (db-path *db*) (equal (native-path file) (native-path (db-path *db*))))
    (error 'persistence-error
           :message "That file is the snapshot of the open database. Use checkpoint instead."))
  (get-lock *file-lock*
    (multiple-value-bind (root seq) (committed-state)
      (write-snapshot file root seq))))

;; Replace all tables with those in FILE. Return the table names.
(defun load-db (file)
  (when *txn*
    (error 'transaction-error :message "Can't load a database file inside a transaction"))
  (when (db-wal *db*)
    (error 'persistence-error :message "Can't load a file into an open database. Call close-db first."))
  (get-lock *file-lock*
    (multiple-value-bind (root seq) (read-snapshot file)
      (with-writer
        (setf (db-root *db*) root
              (db-seq *db*) seq))
      (show-tables))))
