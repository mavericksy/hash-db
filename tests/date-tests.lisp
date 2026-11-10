(in-package #:hash-db-tests)

(in-suite hash-db-suite)

(defun ut (text) (parse-timestamp text))

;;; reading and writing

(def-test timestamp-parses-dates-and-times (:fixture clean-db)
  (is (= 0 (ut "1900-01-01")))
  (is (= 86400 (ut "1900-01-02")))
  (is (= (encode-universal-time 0 0 0 31 1 2024 0) (ut "2024-01-31")))
  (is (= (encode-universal-time 30 45 12 31 1 2024 0) (ut "2024-01-31 12:45:30")))
  (is (= (ut "2024-01-31 12:45:30") (ut "2024-01-31T12:45:30")))
  (is (= (ut "2024-01-31 12:45:30") (ut "2024-01-31T12:45:30Z")))
  (is (= (ut "2024-01-31 12:45:00") (ut "2024-01-31 12:45")))
  (is (= (ut "2024-01-31") (ut "  2024-01-31  ")))
  (is (= (encode-universal-time 59 59 23 31 12 2099 0) (ut "2099-12-31 23:59:59"))))

(def-test timestamp-leap-years (:fixture clean-db)
  (is (= 86400 (- (ut "2000-03-01") (ut "2000-02-29"))) "2000 is a leap year")
  (is (= 86400 (- (ut "2024-03-01") (ut "2024-02-29"))))
  (is (= 86400 (- (ut "2100-03-01") (ut "2100-02-28"))) "2100 is not a leap year, so Feb 28 is followed by Mar 1")
  (is (= (* 366 86400) (- (ut "2025-01-01") (ut "2024-01-01")))))

(def-test timestamp-rejects-bad-text (:fixture clean-db)
  (dolist (text '("" "2024" "2024-1-31" "24-01-31" "2024/01/31" "2024-01-3" "2024-13-01" "2024-00-10"
                  "2024-01-00" "2024-01-32" "2024-02-30" "2023-02-29" "2100-02-29" "1899-12-31"
                  "2024-01-31 25:00" "2024-01-31 12:60" "2024-01-31 12:00:60" "2024-01-31 1:00"
                  "2024-01-31 12" "2024-01-31 12:0" "2024-01-31x12:00" "2024-01-31 12:00:5"
                  "2024-01-31 12:00:00 extra" "tomorrow" "2024-01-31 ab:cd"))
    (signals query-error (parse-timestamp text) "~S" text))
  (signals query-error (parse-timestamp 5))
  (signals query-error (parse-timestamp nil))
  (finishes (parse-timestamp "2000-02-29"))
  (finishes (parse-timestamp "2024-02-29")))

(def-test timestamp-formats (:fixture clean-db)
  (is (equal "1900-01-01" (format-date 0)))
  (is (equal "1900-01-01 00:00:00" (format-timestamp 0)))
  (is (equal "2024-01-31" (format-date (ut "2024-01-31 23:59:59"))))
  (is (equal "2024-01-31 12:45:30" (format-timestamp (ut "2024-01-31 12:45:30"))))
  (is (equal "0999" (subseq (format nil "~4,'0D" 999) 0 4)))
  (dolist (text '("1900-01-01 00:00:00" "1999-12-31 23:59:59" "2000-02-29 12:00:00" "2024-07-04 04:05:06"
                  "2099-12-31 23:59:59" "2400-02-29 00:00:00"))
    (is (equal text (format-timestamp (ut text))) "round trip of ~A" text)))

;;; the expressions

(defun date-value (expr)
  (getf (first (row-list (select :from "one" :col `((as ,expr :v))))) :v))

(defun make-one ()
  (make-table "one" '((:id primary-key-column) (:at timestamp) (:s string)))
  (insert-row (list :at (ut "2024-03-15 08:09:10") :s "2024-03-15") "one"))

(def-test date-expressions (:fixture clean-db)
  (make-one)
  (is (= (ut "2024-03-15") (date-value '(:date "2024-03-15"))))
  (is (= (ut "2024-03-15") (date-value '(:date :s))))
  (is (= (ut "2024-03-15 08:09:10") (date-value '(:timestamp "2024-03-15T08:09:10Z"))))
  (is (equal "2024-03-15" (date-value '(:date-text :at))))
  (is (equal "2024-03-15 08:09:10" (date-value '(:timestamp-text :at))))
  (is (equal '(2024 3 15 8 9 10)
             (list (date-value '(:year :at)) (date-value '(:month :at)) (date-value '(:day :at))
                   (date-value '(:hour :at)) (date-value '(:minute :at)) (date-value '(:second :at)))))
  (is (= (+ (ut "2024-03-15 08:09:10") 86400) (date-value '(:+ :at 86400))) "a day later"))

(def-test day-of-the-week (:fixture clean-db)
  (make-table "one" '((:id primary-key-column) (:at timestamp)))
  (loop for (text dow) in '(("2023-12-31" 0) ("2024-01-01" 1) ("2024-01-02" 2) ("2024-01-03" 3)
                            ("2024-01-04" 4) ("2024-01-05" 5) ("2024-01-06" 6) ("1900-01-01" 1))
        do (insert-row (list :at (ut text)) "one"))
  (is (equal '(0 1 2 3 4 5 6 1) (mapcar (lambda (r) (getf r :v))
                                         (row-list (select :from "one" :col '((as (:dow :at) :v)) :order-by :id))))))

(def-test date-expressions-and-null (:fixture clean-db)
  (make-table "one" '((:id primary-key-column) (:at timestamp) (:s string)))
  (insert-row '() "one")
  (dolist (expr '((:date :s) (:timestamp :s) (:date-text :at) (:timestamp-text :at) (:year :at) (:month :at)
                  (:day :at) (:hour :at) (:minute :at) (:second :at) (:dow :at)))
    (is (eq :null (date-value expr)) "~S of NULL" expr)))

(def-test date-expression-errors (:fixture clean-db)
  (make-one)
  (signals query-error (date-value '(:date "2024-02-30")))
  (signals query-error (date-value '(:date "soon")))
  (signals query-error (date-value '(:date 5)))
  (signals query-error (date-value '(:year "x")))
  (signals query-error (date-value '(:year -5)))
  (signals query-error (date-value '(:date)))
  (signals query-error (date-value '(:today 1))))

(def-test today-uses-the-clock (:fixture clean-db)
  (make-one)
  (with-clock ((+ (ut "2024-05-06") 12345))
    (is (= (ut "2024-05-06") (date-value '(:today))))
    (is (= (ut "2024-05-06") (hash-db::today))))
  (with-clock ((ut "2024-05-06 23:59:59"))
    (is (= (ut "2024-05-06") (date-value '(:today)))))
  (with-clock ((ut "2024-05-07 00:00:00"))
    (is (= (ut "2024-05-07") (date-value '(:today))))))

(def-test dates-in-where-use-the-index (:fixture clean-db)
  (make-table "events" '((:id primary-key-column) (:at timestamp) (:what string)))
  (create-index "events" :at)
  (loop for day from 1 to 28
        do (insert-row (list :at (ut (format nil "2024-02-~2,'0D 10:00:00" day)) :what (format nil "e~D" day)) "events"))
  (is (equal '(10 11 12) (mapcar (lambda (r) (getf r :id))
                                 (row-list (select :from "events"
                                                   :where '(between :at (:date "2024-02-10") (:timestamp "2024-02-12 23:59:59"))
                                                   :order-by :id)))))
  (let ((step (first (explain :from "events" :where '(>= :at (:date "2024-02-27"))))))
    (is (equal '(:index :range 2) (list (getf (cddr step) :access) (getf (cddr step) :op) (getf (cddr step) :rows)))
        "the constant is worked out first, so the index is used")))

(def-test dates-in-group-and-order (:fixture clean-db)
  (make-table "events" '((:id primary-key-column) (:at timestamp)))
  (dolist (text '("2023-12-31" "2024-01-05" "2024-01-20" "2024-02-02" "2024-02-03" "2024-02-04"))
    (insert-row (list :at (ut text)) "events"))
  (is (equal '((:y 2023 :m 12 :n 1) (:y 2024 :m 1 :n 2) (:y 2024 :m 2 :n 3))
             (row-list (select :from "events"
                               :group-by '((:year :at) (:month :at))
                               :col '((as (:year :at) :y) (as (:month :at) :m) (as (count) :n))
                               :order-by '((:year :at) (:month :at)))))))
