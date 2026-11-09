;;;; Write-ahead log. SBCL only.
;;;;
;;;; An open database is a snapshot file PATH and a log file PATH.wal.
;;;; Every committed transaction is appended to the log and flushed before it
;;;; becomes visible. After a crash, OPEN-DB loads the snapshot and replays
;;;; the log.
;;;;
;;;; A log record is a frame: 4 bytes length, 4 bytes checksum, then the
;;;; payload. The payload is the text of (seq entries), as UTF-8. A crash can
;;;; leave a partial frame at the end. Replay stops at the first frame that is
;;;; incomplete, fails its checksum or does not parse. It cuts the file there.

(in-package :hash-db)

(defstruct (wal (:constructor make-wal (stream path sync checkpoint-bytes bytes)))
  stream
  path
  ;; :always flushes to disk at every commit. :never leaves it to the OS.
  sync
  ;; the log is folded into the snapshot when it grows past this. Nil means never.
  checkpoint-bytes
  ;; size of the file
  bytes
  ;; true after a failed write that could not be undone
  (broken nil))

(defun log-path (path)
  (uiop:parse-native-namestring (concatenate 'string (native-path path) ".wal")))

;;; Frames

;; FNV-1a, 32 bit
(defun checksum (octets)
  (let ((h #x811c9dc5))
    (loop for b across octets
          do (setf h (logand #xffffffff (* (logxor h b) #x01000193))))
    h))

(defun put-uint32 (n vector offset)
  (loop for i from 0 below 4
        do (setf (aref vector (+ offset i)) (ldb (byte 8 (* 8 (- 3 i))) n))))

(defun get-uint32 (vector offset)
  (loop with n = 0
        for i from 0 below 4
        do (setf n (logior (ash n 8) (aref vector (+ offset i))))
        finally (return n)))

(defmacro with-log-syntax (&body body)
  `(let ((*package* (find-package :keyword))
         (*read-default-float-format* 'single-float)
         (*read-eval* nil)
         (*print-readably* t)
         (*print-pretty* nil)
         (*print-circle* nil))
     ,@body))

(defun encode-frame (seq entries)
  (let* ((text (handler-case (with-log-syntax (prin1-to-string (list seq entries)))
                 (print-not-readable (c)
                   (error 'persistence-error
                          :message (format nil "Can't log this change. It holds a value that can't be printed and read back: ~A" c)))))
         (payload (sb-ext:string-to-octets text :external-format :utf-8))
         (frame (make-array (+ 8 (length payload)) :element-type '(unsigned-byte 8))))
    (put-uint32 (length payload) frame 0)
    (put-uint32 (checksum payload) frame 4)
    (replace frame payload :start1 8)
    frame))

(defun parse-payload (payload)
  (handler-case
      (with-log-syntax
        (let ((record (read-from-string (sb-ext:octets-to-string payload :external-format :utf-8))))
          (and (consp record) (integerp (first record)) (listp (second record)) record)))
    (error () nil)))

;; Read all good records of the log at PATH.
;; Return the records, the size of the good part, and the size of the file.
(defun read-wal-records (path)
  (with-open-file (in path :element-type '(unsigned-byte 8))
    (let ((size (file-length in))
          (good 0)
          (records '())
          (header (make-array 8 :element-type '(unsigned-byte 8))))
      (loop
        (unless (= 8 (read-sequence header in))
          (return))
        (let ((len (get-uint32 header 0))
              (sum (get-uint32 header 4)))
          (when (> len (- size good 8))
            (return))
          (let ((payload (make-array len :element-type '(unsigned-byte 8))))
            (unless (= len (read-sequence payload in))
              (return))
            (unless (= sum (checksum payload))
              (return))
            (let ((record (parse-payload payload)))
              (unless record
                (return))
              (push record records)
              (incf good (+ 8 len))))))
      (values (nreverse records) good size))))

(defun truncate-file (path bytes)
  (sb-posix:truncate (native-path path) bytes))

;; Apply the log at PATH to ROOT, a database that is at transaction SEQ.
;; Records at or below SEQ are already in the snapshot and are skipped.
;; Cut off a damaged tail. Return the new root and the new SEQ.
(defun replay-wal (path root seq)
  (multiple-value-bind (records good size) (read-wal-records path)
    (dolist (record records)
      (destructuring-bind (rec-seq entries) record
        (cond ((<= rec-seq seq))
              ((= rec-seq (1+ seq))
               (setf root (replay-entries root entries)
                     seq rec-seq))
              (t
               (error 'persistence-error
                      :message (format nil "The log ~A jumps from transaction ~D to ~D. Data is missing."
                                       path seq rec-seq))))))
    (when (< good size)
      (truncate-file path good)
      (warn 'wal-truncated :file path :bytes (- size good)))
    (values root seq)))

;;; Appending

(defun fsync-stream (stream)
  (sb-posix:fsync (sb-sys:fd-stream-fd stream)))

;; Write one commit to the log. Return when it is on disk (with :sync :always).
;; If the write fails, cut the partial record off and signal persistence-error.
(defun wal-append (wal seq entries)
  (when (wal-broken wal)
    (error 'persistence-error
           :message "The log is damaged by a failed write. Close and open the database again."))
  (let ((frame (encode-frame seq entries))
        (stream (wal-stream wal))
        (start (wal-bytes wal)))
    (handler-case
        (progn (write-sequence frame stream)
               (finish-output stream)
               (when (eq (wal-sync wal) :always)
                 (fsync-stream stream)))
      (error (c)
        (handler-case
            (progn (sb-posix:ftruncate (sb-sys:fd-stream-fd stream) start)
                   (file-position stream start))
          (error () (setf (wal-broken wal) t)))
        (error 'persistence-error
               :message (format nil "Can't write to the log: ~A" c))))
    (incf (wal-bytes wal) (length frame))
    seq))

(defun open-wal-stream (path)
  (open path :direction :output :element-type '(unsigned-byte 8)
             :if-exists :append :if-does-not-exist :create))

;;; Checkpoint
;;;
;;; A checkpoint writes the committed state as a new snapshot and then empties
;;; the log. If a crash comes between the two steps, the log still holds records
;;; that are in the snapshot. Replay skips them by their transaction numbers.

;; Caller holds the writer lock.
(defun write-checkpoint ()
  (let ((wal (db-wal *db*)))
    (write-snapshot (db-path *db*) (db-root *db*) (db-seq *db*))
    (let ((fd (sb-sys:fd-stream-fd (wal-stream wal))))
      (handler-case
          (progn (sb-posix:ftruncate fd 0)
                 (file-position (wal-stream wal) 0)
                 (sb-posix:fsync fd)
                 (setf (wal-bytes wal) 0))
        (error (c)
          (setf (wal-broken wal) t)
          (error 'persistence-error
                 :message (format nil "The snapshot is saved, but the log could not be emptied: ~A" c)))))))

;; Fold the log into the snapshot. Return the transaction number of the snapshot.
(defun checkpoint ()
  (when *txn*
    (error 'transaction-error :message "Can't checkpoint inside a transaction"))
  (with-writer
    (unless (db-wal *db*)
      (error 'persistence-error :message "No database is open"))
    (write-checkpoint)
    (db-seq *db*)))

;; After a commit: checkpoint when the log is too big. The commit is already
;; durable in the log, so a failed checkpoint is only a warning.
(defun maybe-checkpoint (wal)
  (let ((limit (wal-checkpoint-bytes wal)))
    (when (and limit (>= (wal-bytes wal) limit))
      (handler-case (write-checkpoint)
        (error (c)
          (warn "Automatic checkpoint failed. The log keeps growing. ~A" c))))))

;;; Open and close

(defun wal-exists-p (path)
  (and (probe-file (log-path path)) t))

;; Open the database stored at PATH and use it as the current database.
;; The snapshot is PATH. The log is PATH.wal. Files that do not exist are created.
;; From now on every committed transaction is written to the log first.
;;
;; SYNC is :always (flush each commit to disk) or :never (leave it to the OS).
;; CHECKPOINT-BYTES is the log size that triggers an automatic checkpoint.
;; Nil turns automatic checkpoints off.
;; If tables are in memory and no files exist yet, the tables become the first
;; snapshot. If files exist, memory must be empty.
(defun open-db (path &key (sync :always) (checkpoint-bytes (* 8 1024 1024)))
  (check-type sync (member :always :never))
  (check-type checkpoint-bytes (or null (integer 1)))
  (when *txn*
    (error 'transaction-error :message "Can't open a database inside a transaction"))
  (with-writer
    (when (db-wal *db*)
      (error 'persistence-error :message "A database is already open. Call close-db first."))
    (let* ((path (merge-pathnames path))
           (snapshot-p (and (probe-file path) t))
           (wal-p (wal-exists-p path))
           (root (db-root *db*))
           (seq (db-seq *db*)))
      (when (and (or snapshot-p wal-p) (plusp (fset:size root)))
        (error 'persistence-error
               :message (format nil "Can't open ~A: tables are already in memory" path)))
      (when snapshot-p
        (multiple-value-setq (root seq) (read-snapshot path)))
      (when wal-p
        (multiple-value-setq (root seq) (replay-wal (log-path path) root seq)))
      (unless snapshot-p
        (write-snapshot path root seq))
      (let* ((stream (open-wal-stream (log-path path)))
             (wal (make-wal stream (log-path path) sync checkpoint-bytes (file-length stream))))
        (fsync-directory path)
        (setf (db-root *db*) root
              (db-seq *db*) seq
              (db-wal *db*) wal
              (db-path *db*) path
              (db-commit-fn *db*) #'(lambda (seq entries) (wal-append wal seq entries))
              (db-post-commit-fn *db*) #'(lambda () (maybe-checkpoint wal))))
      (show-tables))))

;; Stop logging. The tables stay in memory. With CHECKPOINT true, fold the
;; log into the snapshot first, so the next open starts fast.
(defun close-db (&key (checkpoint t))
  (when *txn*
    (error 'transaction-error :message "Can't close a database inside a transaction"))
  (with-writer
    (let ((wal (db-wal *db*)))
      (when wal
        (when (and checkpoint (plusp (wal-bytes wal)) (not (wal-broken wal)))
          (write-checkpoint))
        (setf (db-wal *db*) nil
              (db-path *db*) nil
              (db-commit-fn *db*) nil
              (db-post-commit-fn *db*) nil)
        (close (wal-stream wal))
        t))))
