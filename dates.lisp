;;;; Dates: universal times as dates, for the expressions.
;;;;
;;;; A timestamp column holds a universal time: whole seconds since 1900-01-01 00:00:00
;;;; UTC. These work on it, in UTC, from 1900 on:
;;;;
;;;;   (date "2024-01-31")                 the time at the start of that day
;;;;   (timestamp "2024-01-31 12:30:00")   the time of that moment. T and a last Z are fine
;;;;   (today)                             the start of today
;;;;   (date-text x)  (timestamp-text x)   "2024-01-31" and "2024-01-31 12:30:00"
;;;;   (year x) (month x) (day x) (hour x) (minute x) (second x)
;;;;   (dow x)                             day of the week, 0 for Sunday to 6 for Saturday
;;;;
;;;; To add time, add seconds: (+ :created 86400) is a day later. NULL gives NULL.

(in-package :hash-db)

(defun days-in-month (year month)
  (case month
    ((1 3 5 7 8 10 12) 31)
    ((4 6 9 11) 30)
    (t (if (and (zerop (mod year 4)) (or (plusp (mod year 100)) (zerop (mod year 400)))) 29 28))))

;; The universal time of TEXT: YYYY-MM-DD, with an optional time HH:MM or HH:MM:SS after a
;; space or a T, and an optional Z at the end. Signal query-error if it is not valid.
(defun parse-timestamp (text)
  (flet ((bad (why)
           (error 'query-error :message (format nil "~S is not a date or a time: ~A. Write YYYY-MM-DD or YYYY-MM-DD HH:MM:SS" text why))))
    (unless (stringp text)
      (bad "it is not text"))
    (let* ((trimmed (string-right-trim "Z" (string-trim " " text)))
           (length (length trimmed)))
      (unless (and (>= length 10)
                   (digit-run-p trimmed 0 4) (char= (char trimmed 4) #\-)
                   (digit-run-p trimmed 5 7) (char= (char trimmed 7) #\-)
                   (digit-run-p trimmed 8 10))
        (bad "the date must be YYYY-MM-DD"))
      (let ((year (parse-integer trimmed :start 0 :end 4))
            (month (parse-integer trimmed :start 5 :end 7))
            (day (parse-integer trimmed :start 8 :end 10))
            (hour 0) (minute 0) (second 0))
        (when (> length 10)
          (unless (member (char trimmed 10) '(#\Space #\T))
            (bad "a space or T must follow the date"))
          (unless (and (member length '(16 19))
                       (digit-run-p trimmed 11 13) (char= (char trimmed 13) #\:)
                       (digit-run-p trimmed 14 16)
                       (or (= length 16)
                           (and (char= (char trimmed 16) #\:) (digit-run-p trimmed 17 19))))
            (bad "the time must be HH:MM or HH:MM:SS"))
          (setf hour (parse-integer trimmed :start 11 :end 13)
                minute (parse-integer trimmed :start 14 :end 16))
          (when (= length 19)
            (setf second (parse-integer trimmed :start 17 :end 19))))
        (unless (>= year 1900) (bad "the year must be 1900 or later"))
        (unless (<= 1 month 12) (bad "the month must be 1 to 12"))
        (unless (<= 1 day (days-in-month year month))
          (bad (format nil "month ~D has no day ~D" month day)))
        (unless (and (<= hour 23) (<= minute 59) (<= second 59))
          (bad "the time is out of range"))
        (encode-universal-time second minute hour day month year 0)))))

(defun digit-run-p (string start end)
  (and (<= end (length string))
       (loop for i from start below end always (digit-char-p (char string i)))))

(defun format-timestamp (time)
  "TIME, a universal time, as YYYY-MM-DD HH:MM:SS in UTC."
  (multiple-value-bind (second minute hour day month year) (decode-universal-time time 0)
    (format nil "~4,'0D-~2,'0D-~2,'0D ~2,'0D:~2,'0D:~2,'0D" year month day hour minute second)))

(defun format-date (time)
  "TIME, a universal time, as YYYY-MM-DD in UTC."
  (multiple-value-bind (second minute hour day month year) (decode-universal-time time 0)
    (declare (ignore second minute hour))
    (format nil "~4,'0D-~2,'0D-~2,'0D" year month day)))

(defun today ()
  "The universal time at the start of today, in UTC."
  (* 86400 (floor (now) 86400)))

(defun require-time (value op)
  (unless (typep value '(integer 0))
    (error 'query-error :message (format nil "~A needs a universal time, a whole number from 0, got ~S" op value)))
  value)

(defmacro define-date-parser (name function)
  `(define-op ,name (args ctx)
     (check-arity ,name args 1)
     (let ((f (compile-expr (first args) ctx)))
       #'(lambda (row)
           (let ((text (funcall f row)))
             (if (nullish-p text) :null (,function text)))))))

(define-date-parser "DATE" parse-timestamp)
(define-date-parser "TIMESTAMP" parse-timestamp)

(defmacro define-date-reader (name (time) &body body)
  `(define-op ,name (args ctx)
     (check-arity ,name args 1)
     (let ((f (compile-expr (first args) ctx)))
       #'(lambda (row)
           (let ((,time (funcall f row)))
             (if (nullish-p ,time)
                 :null
                 (progn (require-time ,time ,name) ,@body)))))))

(define-date-reader "DATE-TEXT" (time) (format-date time))
(define-date-reader "TIMESTAMP-TEXT" (time) (format-timestamp time))
(define-date-reader "YEAR" (time) (nth-value 5 (decode-universal-time time 0)))
(define-date-reader "MONTH" (time) (nth-value 4 (decode-universal-time time 0)))
(define-date-reader "DAY" (time) (nth-value 3 (decode-universal-time time 0)))
(define-date-reader "HOUR" (time) (nth-value 2 (decode-universal-time time 0)))
(define-date-reader "MINUTE" (time) (nth-value 1 (decode-universal-time time 0)))
(define-date-reader "SECOND" (time) (nth-value 0 (decode-universal-time time 0)))
;; Lisp counts the week from Monday = 0. SQL's dow counts from Sunday = 0.
(define-date-reader "DOW" (time) (mod (1+ (nth-value 6 (decode-universal-time time 0))) 7))

(define-op "TODAY" (args ctx)
  (check-arity "TODAY" args 0)
  #'(lambda (row)
      (declare (ignore row))
      (today)))
