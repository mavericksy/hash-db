;;;; History: read the database as it was.
;;;;
;;;;   (keep-history 100)                        ; keep the last 100 versions
;;;;   (with-as-of (:seq 41) (row-list "people"))
;;;;   (with-as-of (:time (- (now) 3600)) (select :from "people"))
;;;;
;;;; Every commit that changes data makes a new version of the database, and the version
;;;; has a number. The old version stays whole, because the new one shares what did not
;;;; change. So reading an old version needs no copy and no replay: the read just uses the old
;;;; root. History is off by default (keep-history 0). Kept versions hold the memory of the
;;;; rows that later changes replaced. History is in memory only. It starts again after
;;;; a restart.

(in-package :hash-db)

;; Keep the last N versions. With N = 0, keep none and forget those kept. The version
;; now is kept at once, so it can be read after the next change.
(defun keep-history (n)
  (unless (typep n '(integer 0))
    (error 'history-error :message (format nil "History keeps a whole number of versions, 0 or more, got ~S" n)))
  (with-writer
    (setf (db-history-limit *db*) n)
    (cond ((zerop n)
           (setf (db-history *db*) nil))
          (t
           (unless (and (db-history *db*) (= (first (first (db-history *db*))) (db-seq *db*)))
             (record-history (db-seq *db*) (db-root *db*)))
           (when (> (length (db-history *db*)) n)
             (setf (db-history *db*) (subseq (db-history *db*) 0 n)))))
    n))

;; The kept versions, newest first, as (:seq n :time t).
(defun history ()
  (mapcar #'(lambda (entry) (list :seq (first entry) :time (second entry)))
          (db-history *db*)))

;; The root of version SEQ, or the newest version made at or before TIME.
(defun history-root (seq time)
  (let ((kept (db-history *db*)))
    (cond ((and seq time)
           (error 'history-error :message "Give :seq or :time, not both"))
          (seq
           (or (third (find seq kept :key #'first))
               (error 'history-error
                      :message (format nil "Version ~A is not kept. Kept: ~A" seq
                                       (if kept
                                           (format nil "~D to ~D" (first (car (last kept))) (first (first kept)))
                                           "none. Call keep-history first")))))
          (time
           (or (third (find-if #'(lambda (entry) (<= (second entry) time)) kept))
               (error 'history-error
                      :message (format nil "No kept version is as old as the time ~A" time))))
          (t (error 'history-error :message "Give :seq or :time")))))

(defun call-with-as-of (fn &key seq time)
  (let ((root (history-root seq time)))
    (when *txn*
      (error 'transaction-error :message "with-as-of can't be used inside a transaction"))
    (let ((*snapshot* root)
          (*as-of* t))
      (funcall fn))))

;; Run BODY against an older version of the database. Give :seq, the number of a kept
;; version, or :time, a universal time. For :time, BODY sees the newest version made at
;; or before it. Reads see the old data, and any change signals transaction-error.
(defmacro with-as-of ((&key seq time) &body body)
  `(call-with-as-of (lambda () ,@body) :seq ,seq :time ,time))
