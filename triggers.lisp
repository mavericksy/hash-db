;;;; Triggers: functions that run when rows change.
;;;;
;;;;   (defun audit (event table old new)
;;;;     (insert-row (list :what (string-downcase (string event)) :table table) "audit"))
;;;;   (create-trigger :audit "people" :after '(:insert :update :delete) 'audit)
;;;;
;;;; The function takes the event, the table name, the old row and the new row. See the
;;;; notes on triggers in hash-db.lisp for what :before and :after triggers may do.
;;;; Give a function name, not a lambda, if the trigger must survive a save or a
;;;; restart. A lambda works in memory, and a save or a logged commit refuses it.

(in-package :hash-db)

;; Make a trigger on TABLE. TIMING is :before or :after. EVENTS is :insert, :update,
;; :delete, or a list of them. FUNCTION is a function or the name of one. Triggers of
;; one kind run in the order they were made. Return the name.
(defun create-trigger (name table timing events function)
  (let ((name (as-keyword name))
        (events (if (listp events) events (list events))))
    (unless (member timing '(:before :after))
      (error 'schema-error :message (format nil "A trigger runs :before or :after, not ~S" timing)))
    (unless (and events (subsetp events '(:insert :update :delete)))
      (error 'schema-error
             :message (format nil "A trigger needs events from :insert, :update and :delete, got ~S" events)))
    (unless (or (functionp function) (and function (symbolp function) (fboundp function)))
      (error 'schema-error
             :message (format nil "A trigger needs a function or the name of one, got ~S" function)))
    (with-transaction
      (let ((tbl (stored-table table)))
        (when (find name (triggers tbl) :key #'trigger-name)
          (error 'schema-error
                 :message (format nil "Table ~A already has a trigger named ~A" (name tbl) name)))
        (put-table (updated-table tbl :triggers (append (triggers tbl)
                                                        (list (make-trigger name timing events function)))))
        (journal-add (list :create-trigger (name tbl) name timing events function))
        name))))

;; Drop a trigger. Return t, or nil if there is none.
(defun drop-trigger (name table)
  (with-transaction
    (let* ((tbl (stored-table table))
           (name (as-keyword name))
           (trigger (find name (triggers tbl) :key #'trigger-name)))
      (when trigger
        (put-table (updated-table tbl :triggers (remove trigger (triggers tbl))))
        (journal-add (list :drop-trigger (name tbl) name))
        t))))

;; The triggers of a table as (:name n :timing t :events e :function f).
(defun show-triggers (table)
  (mapcar #'(lambda (trigger)
              (list :name (trigger-name trigger) :timing (trigger-timing trigger)
                    :events (trigger-events trigger) :function (trigger-function trigger)))
          (triggers (find-table table))))

(defun replay-table (root name entry)
  (let ((tbl (values (fset:lookup root name))))
    (unless (typep tbl 'table)
      (error 'persistence-error
             :message (format nil "Log entry ~S refers to a missing table" entry)))
    tbl))

(define-replay :create-trigger (root table name timing events function)
  (let ((tbl (replay-table root table entry)))
    (fset:with root table
               (updated-table tbl :triggers (append (triggers tbl)
                                                    (list (make-trigger name timing events function)))))))

(define-replay :drop-trigger (root table name)
  (let ((tbl (replay-table root table entry)))
    (fset:with root table
               (updated-table tbl :triggers (remove name (triggers tbl) :key #'trigger-name)))))

;; A file keeps data only. A trigger that holds a function object can't be saved.
(register-save-check
 (lambda (root)
   (dolist (tbl (all-tables root))
     (dolist (trigger (triggers tbl))
       (when (functionp (trigger-function trigger))
         (error 'persistence-error
                :message (format nil "Can't save trigger ~A of table ~A: it holds a function object. Use a function name."
                                 (trigger-name trigger) (name tbl))))))))
