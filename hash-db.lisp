(in-package #:hash-db)
;;;; Simple hash table DB
;;;; Flat file with unique integer ids.
;;;; depends on cl-store ,BT and make-hash & fiveam for the tests
;;;; v.1
;;;; TODO implement relational framework across multiple hash-tables and object
;;;; mapping

(eval-now

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

  #+5am(in-suite database)
  #+5am(test database-in-out ()
             (setf *database* nil)
             (setf *class-check* nil)
             (setf *database* (make-hash:make-hash :test #'equal))
             (dotimes (i 450) (setf (gethash i *database*) i))
             (is (= (gethash 0 *database*) 0))
             (is (= (gethash 449 *database*) 449))
             (setf *database* nil))

  ;; When implementing a class check, make sure you have the class defined.
  ;; and a <CLASS-NAME>-P typep method
  ;;ie (defun <CLASS-NAME>-p (inst) (typep inst '<CLASS-NAME>))
  (defun class-check (inst)
    (let ((str-or-sym))
      (cond ((stringp *database-class*)
             (setf str-or-sym (string-upcase *database-class*)))
            ((symbolp *database-class*)
             (setf str-or-sym (symbol-name *database-class*))))
      (funcall (intern (concatenate 'string str-or-sym "-P")) inst)))

  #+5am(in-suite database)
  #+5am(test database-class-check-test ()
             (defclass testt () (( name :initform "testt")))
             (defmethod testt-p (inst) (typep inst 'testt))
             (let ((a (make-instance 'testt)))
               (setf *class-check* t)
               (setf *database-class* 'testt)
               (is (!null (class-check a))))
             (setf *class-check* nil)
             (clear-database))

  (defun ensure-gethash-local (id inst)
    (if *class-check*
        (cond ((and (class-check inst) (!null id) (numberp id))
               (ensure-gethash id *database* inst))
              (t (error
                   (format nil
                           "Instance must be of ~A and a fixnum id."
                           *database-class*))))
        (if (and (numberp id) (!null id))
            (ensure-gethash id *database* inst))))

  #+5am(in-suite database)
  #+5am(test database-ensure-test ()
             ())

  (defun get-unq-id ()
    (with-input-from-file
      (in *u-id*)
      (with-standard-io-syntax
        (setf *unique-id* (parse-integer (read-line in)))
        *unique-id*)))

  (defun put-unq-id (c)
    (with-output-to-file
      (out *u-id*
           :if-exists :supersede)
      (with-standard-io-syntax
        (format out "~D" c)))
    (get-unq-id))

  (defun get-inc-unq-id ()
    "Lock the database unique-id"
    (get-lock *id-lock*
              (progn
                (get-unq-id)
                (put-unq-id (1+ *unique-id*))
                *unique-id*)))

  #+5am(in-suite database)
  #+5am(test database-unique-id ()
             (is (= (put-unq-id 0) 0))
             (is (= (get-inc-unq-id) 1))
             (is (= (get-unq-id) 1))
             (put-unq-id 0)
             (loop repeat 2 do
                   (bt:make-thread (lambda ()
                                     (loop repeat 3 do (get-inc-unq-id))))
                   (bt:make-thread (lambda ()
                                     (loop repeat 3 do (get-inc-unq-id))
                                     (loop repeat 3 do (get-inc-unq-id)))))
             (sleep 1)
             (is (= (get-unq-id) 18)))

  (defun get-by-key (key)
    (gethash key *database*))

  (defun save-db ()
    (get-lock *write-lock*
              (cl-store:store *database* *filename*)))

  (defun load-db ()
    (get-lock *read-lock*
              (setf *database* (cl-store:restore *filename*))))

  (get-unq-id)

  #+5am(defun clear-database ()
         (setf *database* nil)
         (setf *database* (make-hash:make-hash :test #'equal)))) ; eval-now end
