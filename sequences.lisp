;;;; Sequences: named counters.
;;;;
;;;; A sequence hands out numbers one after another. Use it for order numbers,
;;;; invoice numbers and the like. It lives in the root next to the tables, so it
;;;; follows transactions: a number taken in a transaction that rolls back is taken
;;;; again. The numbers are in order. They have no gaps unless a sequence is
;;;; restarted or dropped.
;;;;
;;;; Use one for a column default with a named function:
;;;;   (create-sequence "order-numbers")
;;;;   (defun next-order-number () (next-value "order-numbers"))
;;;;   (make-table "orders" '((:id primary-key-column) (:no integer :default-fn next-order-number)))

(in-package :hash-db)

;; NEXT is the number the next call gives. ISSUED is true once a number was given.
(defstruct (counter (:constructor make-counter (name next increment &optional issued)))
  name
  next
  increment
  issued)

(defun find-counter (name)
  (let ((object (named-object name)))
    (if (typep object 'counter)
        object
        (error 'no-such-sequence :name name
                                 :message (format nil "The sequence: ~A does not exist" name)))))

;; Make a sequence. The first number is START, then START + INCREMENT, and so on.
;; INCREMENT can be negative.
(defun create-sequence (name &key (start 1) (increment 1))
  (unless (and (integerp start) (integerp increment) (/= increment 0))
    (error 'schema-error
           :message (format nil "A sequence needs a whole START and a whole INCREMENT other than 0, got ~S and ~S"
                            start increment)))
  (with-transaction
    (check-name-free name "sequence")
    (put-object name (make-counter name start increment))
    (journal-add (list :create-sequence name start increment))
    name))

;; Drop a sequence. Return t, or nil if there is none.
(defun drop-sequence (name)
  (with-transaction
    (when (typep (named-object name) 'counter)
      (remove-object name)
      (journal-add (list :drop-sequence name))
      t)))

;; Give the next number.
(defun next-value (name)
  (with-transaction
    (let* ((counter (find-counter name))
           (value (counter-next counter))
           (next (+ value (counter-increment counter))))
      (put-object name (make-counter name next (counter-increment counter) t))
      (journal-add (list :set-sequence name next t))
      value)))

;; The last number given, or nil if none was given yet.
(defun current-value (name)
  (let ((counter (find-counter name)))
    (and (counter-issued counter)
         (- (counter-next counter) (counter-increment counter)))))

;; Make the next number VALUE.
(defun restart-sequence (name value)
  (unless (integerp value)
    (error 'schema-error :message (format nil "A sequence restarts at a whole number, got ~S" value)))
  (with-transaction
    (let ((counter (find-counter name)))
      (put-object name (make-counter name value (counter-increment counter)))
      (journal-add (list :set-sequence name value nil))
      value)))

;; All sequences as (:name n :next x :increment i :issued b).
(defun show-sequences ()
  (let ((root (current-root)))
    (loop for name in (fset:convert 'list (fset:domain root))
          for object = (values (fset:lookup root name))
          when (typep object 'counter)
            collect (list :name name :next (counter-next object)
                          :increment (counter-increment object)
                          :issued (and (counter-issued object) t)))))

(define-replay :create-sequence (root name start increment)
  (fset:with root name (make-counter name start increment)))

(define-replay :drop-sequence (root name)
  (fset:less root name))

(define-replay :set-sequence (root name next issued)
  (let ((counter (values (fset:lookup root name))))
    (unless (typep counter 'counter)
      (error 'persistence-error
             :message (format nil "Log entry sets the missing sequence ~A" name)))
    (fset:with root name (make-counter name next (counter-increment counter) issued))))

(register-snapshot-section
 :sequences
 (lambda (root)
   (loop for name in (fset:convert 'list (fset:domain root))
         for object = (values (fset:lookup root name))
         when (typep object 'counter)
           collect (list name (counter-next object) (counter-increment object)
                         (counter-issued object))))
 (lambda (root data)
   (loop for (name next increment issued) in data
         do (setf root (fset:with root name (make-counter name next increment issued))))
   root))
