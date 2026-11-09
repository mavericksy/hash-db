;;;; Expiry: rows that live for a limited time.
;;;;
;;;;   (make-table "sessions" '((:id primary-key-column) (:user string) (:expires timestamp)))
;;;;   (set-ttl "sessions" :expires)
;;;;   (insert-row (list :user "ann" :expires (expires-in 3600)) "sessions")
;;;;   (purge-expired)           ; delete the rows whose time has come. :table limits it to one table.
;;;;   (start-sweeper :interval 60)   ; or do it in the background
;;;;
;;;; A row whose timestamp is at or before the time now has expired. A row with NULL
;;;; there never expires. Expired rows stay in the table, and queries still see
;;;; them, until a purge deletes them. To hide them at once, add
;;;; (> :expires (fn get-universal-time)) to :where.
;;;;
;;;; A purge is a delete-where, so cascades and triggers run, and it is logged.
;;;; set-ttl makes an index on the column, so a purge reads only the expired rows.

(in-package :hash-db)

;; The time. Universal time, in whole seconds. Bind or set it to test expiry.
(defvar *clock* #'get-universal-time)

(defun now ()
  (funcall *clock*))

;; A timestamp SECONDS from now.
(defun expires-in (seconds)
  (+ (now) seconds))

;; Make the timestamp column COLUMN of TABLE say when each row expires.
(defun set-ttl (table column)
  (with-transaction
    (let* ((tbl (stored-table table))
           (col (find-column column (schema tbl))))
      (unless (eq (second (spec col)) 'timestamp)
        (error 'schema-error
               :message (format nil "Column ~A of table ~A is not a timestamp column" (name col) (name tbl))))
      (unless (table-index-for tbl (name col))
        (create-index (name tbl) (name col)))
      (put-table (updated-table (stored-table table) :ttl (name col)))
      (journal-add (list :set-ttl (name tbl) (name col)))
      (name col))))

;; Stop expiring the rows of TABLE. The index stays.
(defun clear-ttl (table)
  (with-transaction
    (let ((tbl (stored-table table)))
      (when (ttl tbl)
        (put-table (updated-table tbl :ttl nil))
        (journal-add (list :set-ttl (name tbl) nil))
        t))))

;; The column that says when rows of TABLE expire, or nil.
(defun ttl-column (table)
  (ttl (find-table table)))

;; Delete the expired rows of :table, or of every table that has a ttl. Return how
;; many rows were deleted directly. :now defaults to the time now.
(defun purge-expired (&key table (now (now)))
  (with-transaction
    (loop for tbl in (if table (list (stored-table table)) (all-tables))
          for column = (ttl tbl)
          when column
            sum (delete-where (name tbl) :where (list '<= column now)))))

(define-replay :set-ttl (root table column)
  (let ((tbl (replay-table root table entry)))
    (fset:with root table (updated-table tbl :ttl column))))

;;; The sweeper: a thread that purges every INTERVAL seconds.

(defstruct (sweeper (:constructor make-sweeper ()))
  thread
  (stop nil)
  (lock (bt:make-lock "SWEEPER-LOCK"))
  (wake (bt:make-condition-variable)))

;; Start a thread that calls purge-expired every INTERVAL seconds on the database that
;; is current now. Return t. There is one sweeper per database. If a purge fails,
;; ON-ERROR is called with the condition in the sweeper thread, and the sweeper goes on.
;; The default warns.
(defun start-sweeper (&key (interval 60)
                        (on-error #'(lambda (c) (warn "The sweeper could not purge: ~A" c))))
  (unless (and (realp interval) (plusp interval))
    (error 'schema-error :message (format nil "The sweeper interval must be a positive number, got ~S" interval)))
  (when (db-sweeper *db*)
    (error 'schema-error :message "The sweeper is already running. Call stop-sweeper first."))
  (let ((sweeper (make-sweeper))
        (db *db*))
    (setf (sweeper-thread sweeper)
          (bt:make-thread
           (lambda ()
             (let ((*db* db))
               (loop
                 (bt:with-lock-held ((sweeper-lock sweeper))
                   (unless (sweeper-stop sweeper)
                     (bt:condition-wait (sweeper-wake sweeper) (sweeper-lock sweeper)
                                        :timeout interval))
                   (when (sweeper-stop sweeper)
                     (return)))
                 (handler-case (purge-expired)
                   (error (c) (funcall on-error c))))))
           :name "hash-db sweeper"))
    (setf (db-sweeper db) sweeper)
    t))

;; Stop the sweeper and wait for it. Return t, or nil if none was running.
(defun stop-sweeper ()
  (let ((sweeper (db-sweeper *db*)))
    (when sweeper
      (bt:with-lock-held ((sweeper-lock sweeper))
        (setf (sweeper-stop sweeper) t)
        (bt:condition-notify (sweeper-wake sweeper)))
      (bt:join-thread (sweeper-thread sweeper))
      (setf (db-sweeper *db*) nil)
      t)))
