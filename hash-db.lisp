;;;; Flat files with primary-key and foreign key constraint ids.
;;;; depends on cl-store ,BT, alexandria, iterate and make-hash
;;;; v.1

(in-package :hash-db)

;; Return a t or nil on thing output. 
;; when I dont need the output, just a truthiness.
(defun !null (thing)
  (not (null thing)))

;; Default #'equal for hash-tables
(defun create-hash-table (&key (test #'equal))
  (make-hash:make-hash :test test))

; Global database table
(defparameter *global-tables* (create-hash-table))
(defun show-tables ()
  (iter (for (k v) in-hashtable *global-tables*) (collect k)))

;; Push a tabe to the database, no duplicates
(defun add-table-to-global (db name)
  (ensure-gethash name *global-tables* db))

;; Return a table of NAME
(defun get-named-table (name)
  (gethash name *global-tables*))

;; read and write lock for file ops
(defparameter *write-lock* (bt:make-lock "WRITE-LOCK"))
(defparameter *read-lock* (bt:make-lock "READ-LOCK"))

;; Internal locking on tables.
;; The tables lock is a slot
(defmacro get-lock (lock &body body)
  `(bt:with-lock-held (,lock)
     ,@body))

(locally (declare (optimize safety))
         ;; A table defined with a unique name and schema
         ;; table locking is implemented and rows are hash-tables
         (defclass table ()
           ((primary-key   :accessor pk
                           :initform 0
                           :type integer)
            (name          :accessor name
                           :initarg         :name
                           :type keyword)
            (row           :accessor rows
                           :initarg         :rows
                           :initform        (make-rows))
            (schema        :accessor schema
                           :initarg         :schema)
            (lock          :accessor lock
                           :initarg         :lock))
           (:documentation "A table in a database."))

         (defmethod get-pk ((table table))
           (setf (pk table) (1+ (pk table))))

         (defmethod get-schema ((table table))
           (schema table))

         (defmethod add-column ((table table) col-def)
           (pushnew (apply #'make-col col-def) (schema table))))

;; Make an instance of a table with a hash-table rowset
(defun create-table-instance (name schema)
  (make-instance 'table :name name :schema schema :lock (bt:make-lock name)))

(defun make-rows ()
  (create-hash-table))

(defun not-nullable (val col)
  (or val (error "Column ~A can't be null" (name col))))

;; Create a class specific column to store instances of that class
;;
(defmacro make-class-specific-col (&key type
                                   class
                                   (comparator       nil comparator-p)
                                   (eq-pred          nil eq-pred-p)
                                   (val-norm         nil val-norm-p)
                                   (references       nil references-p)
                                   (constraint-check nil constraint-check-p))
  (declare (ignore references))
  `(defmethod make-col (name (type (eql ',type)) &optional default-value references)
     (make-instance
       ',class
       :name name
       ,@(when comparator-p       `(:comparator ,comparator))
       ,@(when eq-pred-p          `(:equality-predicate ,eq-pred))
       ,@(when val-norm-p         `(:value-normaliser ,val-norm))
       ,@(when references-p       `(:references references))
       ,@(when constraint-check-p `(:constraint-check ,constraint-check))
       :default-value default-value)))

(locally (declare (optimize safety))
         (defclass column ()
           ((name                :reader name
                                 :initarg                    :name)
            (equality-predicate  :reader equality-predicate
                                 :initarg                    :equality-predicate
                                 :initform nil)
            (comparator          :reader comparator
                                 :initarg                    :comparator)
            (default-value       :reader default-value
                                 :initarg                    :default-value
                                 :initform nil)
            (value-normaliser    :reader value-normaliser
                                 :initarg                    :value-normaliser
                                 :initform #'(lambda (v col)
                                               (declare (ignore col)) v)))
           (:documentation "The column definition for a table column"))

         (defclass primary-key-column (column)
           ((value-normaliser :initform #'not-nullable)))

         (defclass foreign-key-column (column)
           ((references       :reader references
                              :initarg :references
                              :initform (error "Must supply a reference table for a foreign key column."))
            (foreign-column   :reader foreign-column
                              :initform :primary-key)
            (equality-predicate :initform #'=)
            (value-normaliser :initform #'not-nullable)
            (constraint-check :reader constraint-check 
                              :initform 
                              #'(lambda (primary-tbl fk)
                                  (if (and 
                                        (!null 
                                          (gethash 
                                            primary-tbl 
                                            *global-tables*)) 
                                        (!null 
                                          (gethash fk
                                                   (rows 
                                                     (gethash 
                                                       primary-tbl 
                                                       *global-tables*)))))
                                      fk
                                      (error "Foreign key constraint violation")))))))

(defclass interned-values-column (column)
  ((interned-values    :reader interned-values
                       :initform (create-hash-table))
   (equality-predicate :initform #'eql)
   (value-normaliser   :initform #'intern-for-column)))

(defun intern-for-column (val col)
  (let ((hash (interned-values col)))
    (or (gethash (not-nullable val col) hash)
        (setf (gethash val hash) val))))

(defgeneric make-col (name type &optional default-value references))

;; A couple default column definitions
;;
(make-class-specific-col :class column 
                         :type boolean 
                         :eq-pred #'=)
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

(defun make-table (name schema)
  (add-table-to-global (create-table-instance name (make-schema schema)) name))

(locally (declare (optimize (speed 3)))
         (defun insert-row (name-and-values table)
           (let ((tbl (gethash table *global-tables*)))
             (unless tbl (error "The table: ~A does not exist" table))
             (get-lock (lock tbl)
               (let ((id (get-pk tbl)))
                 (ensure-gethash id (rows tbl) 
                                 (normalise-row name-and-values (schema tbl))))))))

(defun normalise-row (names-and-values schema)
  (iter (for col in schema)
        (with nv = names-and-values)
        (if (equal :foreign-key (name col)) 
            (progn (collect (name col)) 
                   (collect (funcall (constraint-check col) 
                                     (references (find :foreign-key schema :key #'name)) 
                                     (getf nv :foreign-key))))
            (let ((col-val (or (getf nv (name col)) (default-value col))))
              (collect (name col))
              (collect (normalise-for-col col-val col))))))

(defun normalise-for-col (val col)
  (funcall (value-normaliser col) val col))

(defun select (&key (col t) from where distinct order-by inner-join)
  (let ((rows (rows from))
        (schema (schema from)))
    ;;
    (when inner-join
      ())
    ;;
    (when where
      (setf rows (restrict-rows rows where)))
    ;;
    (unless (eql col t)
      (setf schema (extract-schema (mklist col) schema))
      (setf rows (project-columns rows schema)))
    ;;
    (when distinct
      (setf rows (distinct-rows rows schema)))
    ;;
    (when order-by
      (setf rows (sorted-rows rows schema (mklist order-by))))
    ;;
    (make-instance 'table :rows rows :schema schema :name (gensym))))

(defun mklist (thing)
  (if (listp thing) thing (list thing)))

(defun extract-schema (col-names schema)
  (iter (for c in col-names) (collect (find-column c schema))))

(defun find-column (col-name schema)
  (or (find col-name schema :key #'name)
      (error "No column: ~A in schema: ~A" col-name schema)))

(defun restrict-rows (rows where)
  (iter (for (k v) in-hashtable rows) (when (funcall where v) (collect v))))

(defun project-columns (rows schema)
  (map 'vector (extractor schema) rows))

(defun distinct-rows (rows schema)
  (remove-duplicates rows :test (row-equality-tester schema)))

(defun sorted-rows (rows schema order-by)
  (sort (copy-seq rows) (row-comparator order-by schema)))

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

(defun row-comparator (col-names schema)
  (let ((comparators (mapcar #'comparator (extract-schema col-names schema))))
    #'(lambda (a b)
        (iter (for name in col-names)
              (for comp in comparators)
              (with a-v = (getf a name))
              (with b-v = (getf b name))
              (always (funcall comp a-v b-v))
              (thereis (funcall comp b-v a-v))))))

(defun column-matcher (col val)
  (let ((name (name col))
        (predicate (equality-predicate col))
        (normalised (normalise-for-col val col)))
    #'(lambda (row) (funcall predicate (getf row name) normalised))))

(defun column-matchers (schema names-and-values)
  (iter (for (name val) on names-and-values by #'cddr)
        (when val
          (collect (column-matcher (find-column name schema) val)))))

(defun matching (table &rest names-and-values)
  (let ((matchers (column-matchers (schema table) names-and-values)))
    #'(lambda (row) (every #'(lambda (matcher) (funcall matcher row)) matchers))))

(defun save-db (db file)
  (get-lock *write-lock*
    (bt:make-thread
      (lambda ()
        (cl-store:store db file)))))

(defun load-db (db file)
  (get-lock *read-lock*
    (bt:make-thread
      (lambda ()
        (setf db (cl-store:restore file))))))
