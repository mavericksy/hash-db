(in-package #:hash-db)
;;;; Simple hash table DB
;;;; Flat file with unique integer ids.
;;;; depends on cl-store ,BT and make-hash
;;;; v.1
;;;; TODO implement relational framework across multiple hash-tables and object
;;;; mapping

(defun !null (something)
  (not (null something)))

;; Should the database ensure a specific class or not?
(defparameter *class-check* nil)
; What is that class?
(defparameter *database-class* nil)

(defparameter *database* (make-hash:make-hash :test #'equal))

(defun get-database ()
  *database*)

(defparameter *write-lock* (bt:make-lock))
(defparameter *read-lock* (bt:make-lock))
(defparameter *id-lock* (bt:make-lock))
(defparameter *unique-id* nil)

(defmacro get-lock (lock &body body)
  `(bt:with-lock-held (,lock)
                      ,@body))

;; Create these files
;; File creation..
(defvar *filename*
  (make-pathname :directory '(:relative "db") :name "database" :type "db"))
(defvar *u-id* (make-pathname :directory '(:relative "db") :name "unq" :type "id"))

;; When implementing a class check, make sure you have the class defined.
;; and a <CLASS-NAME>-P typep method
;;ie (defun <CLASS-NAME>-p (inst) (typep inst '<CLASS-NAME>))
(defun class-check (inst)
  (declare (string str-or-sym))
  (let ((str-or-sym))
    (cond ((stringp *database-class*)
           (setf str-or-sym (string-upcase *database-class*)))
          ((symbolp *database-class*)
           (setf str-or-sym (symbol-name *database-class*))))
    (funcall (intern (concatenate 'string str-or-sym "-P")) inst)))

(defun ensure-gethash-local (id inst)
  (declare (fixnum id))
  (declare (optimize (speed 3) (safety 0)))
  (if *class-check*
      (cond ((and (class-check inst) (!null id) (numberp id))
             (ensure-gethash id *database* inst))
            (t (error
                 (format nil
                         "Instance must be of ~A and a fixnum id."
                         *database-class*))))
      (if (and (numberp id) (!null id))
          (ensure-gethash id *database* inst)))
  (save-db))

(defun get-unq-id ()
  (with-input-from-file
    (in *u-id*)
    (with-standard-io-syntax
      (setf *unique-id* (parse-integer (read-line in)))
      *unique-id*)))

(defun put-unq-id (c)
  (declare (fixnum c))
  (with-output-to-file
    (out *u-id*
         :if-exists :supersede)
    (with-standard-io-syntax
      (format out "~D" c)))
  (get-unq-id))

(defun get-inc-unq-id ()
  "Lock the database unique-id"
  (declare (fixnum *unique-id*))
  (get-lock *id-lock*
            (progn
              (get-unq-id)
              (put-unq-id (the fixnum (1+ *unique-id*))))
              (the fixnum *unique-id*)))

(defun get-by-key (key)
  (gethash key *database*))

(defun save-db ()
  (get-lock *write-lock*
            (cl-store:store *database* *filename*)))

(defun load-db ()
  (get-lock *read-lock*
            (setf *database* (cl-store:restore *filename*))))

(get-unq-id)
