;;;; Conditions signaled by hash-db.
;;;; Handle HASH-DB-ERROR to catch every error that hash-db signals itself.

(in-package :hash-db)

(define-condition hash-db-error (error)
  ((message :initarg :message :reader error-message :initform "hash-db error"))
  (:report (lambda (condition stream)
             (write-string (error-message condition) stream)))
  (:documentation "Parent of all hash-db errors."))

;;; Missing things

(define-condition no-such-table (hash-db-error)
  ((table :initarg :table :reader error-table)))

(define-condition no-such-column (hash-db-error)
  ((column :initarg :column :reader error-column)))

(define-condition no-such-row (hash-db-error)
  ((table :initarg :table :reader error-table)
   (id    :initarg :id    :reader error-id)))

;;; Bad definitions

(define-condition schema-error (hash-db-error) ()
  (:documentation "A table or column definition is not valid."))

(define-condition table-exists (schema-error)
  ((table :initarg :table :reader error-table)))

;;; Bad input

(define-condition invalid-row (hash-db-error) ()
  (:documentation "A row given to insert-row or update-row is not valid."))

(define-condition query-error (hash-db-error) ()
  (:documentation "A query is not valid."))

(define-condition transaction-error (hash-db-error) ()
  (:documentation "A transaction function was used in the wrong place."))

;;; Constraints

(define-condition constraint-violation (hash-db-error)
  ((table  :initarg :table  :reader error-table  :initform nil)
   (column :initarg :column :reader error-column :initform nil))
  (:documentation "A change would break a rule of the schema."))

(define-condition not-null-violation (constraint-violation) ())
(define-condition type-mismatch (constraint-violation) ())
(define-condition unique-violation (constraint-violation) ())
(define-condition check-violation (constraint-violation) ())
(define-condition foreign-key-violation (constraint-violation) ())

;;; Files

(define-condition persistence-error (hash-db-error) ()
  (:documentation "Data can't be saved or restored."))

(define-condition wal-truncated (warning)
  ((file  :initarg :file  :reader error-file)
   (bytes :initarg :bytes :reader truncated-bytes))
  (:report (lambda (condition stream)
             (format stream "Dropped ~D bytes of a damaged log tail from ~A"
                     (truncated-bytes condition) (error-file condition))))
  (:documentation "The log ended in a partial or damaged record. The damage was cut off."))

(define-condition file-format-error (hash-db-error)
  ((file :initarg :file :reader error-file)))
