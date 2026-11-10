(in-package #:hash-db-tests)

(in-suite hash-db-suite)

(defun make-sql-events ()
  (sql-script "CREATE TABLE events (id INTEGER PRIMARY KEY, what TEXT, at TIMESTAMP, day DATE);
               INSERT INTO events (what, at, day) VALUES
                 ('new year', TIMESTAMP '2024-01-01 00:00:00', DATE '2024-01-01'),
                 ('lunch',    TIMESTAMP '2024-01-01 12:30:00', DATE '2024-01-01'),
                 ('leap',     TIMESTAMP '2024-02-29 08:00:00', DATE '2024-02-29'),
                 ('summer',   TIMESTAMP '2024-07-04T15:45:10Z', DATE '2024-07-04'),
                 ('unknown',  NULL, NULL)"))

(def-test sql-date-literals (:fixture clean-db)
  (make-sql-events)
  (is (equal (list (ut "2024-01-01") (ut "2024-02-29"))
             (mapcar (lambda (r) (getf r :day))
                     (sq "SELECT day FROM events WHERE what IN ('new year', 'leap') ORDER BY id"))))
  (is (equal '("lunch" "leap")
             (mapcar (lambda (r) (getf r :what))
                     (sq "SELECT what FROM events WHERE at > TIMESTAMP '2024-01-01 12:00:00' AND day < DATE '2024-03-01' ORDER BY id"))))
  (is (equal '("new year" "lunch" "leap" "summer")
             (mapcar (lambda (r) (getf r :what))
                     (sq "SELECT what FROM events WHERE day BETWEEN DATE '2024-01-01' AND DATE '2024-12-31' ORDER BY id"))))
  (signals query-error (sq "SELECT what FROM events WHERE day BETWEEN DATE '2024-01-01' AND '2024-12-31'")
           "a string is not a date, so it can not be compared with one"))

(def-test sql-date-functions (:fixture clean-db)
  (make-sql-events)
  (is (equal '((:what "leap" :d "2024-02-29" :t "2024-02-29 08:00:00"))
             (sq "SELECT what, date_text(at) AS d, timestamp_text(at) AS t FROM events WHERE what = 'leap'")))
  (is (equal '((:y 2024 :m 7 :d 4 :h 15 :mi 45 :s 10 :dw 4))
             (sq "SELECT year(at) AS y, month(at) AS m, day(at) AS d, hour(at) AS h, minute(at) AS mi,
                         second(at) AS s, dow(at) AS dw FROM events WHERE what = 'summer'")))
  (is (equal '((:y 2024 :m 2 :d 29 :h 8 :mi 0 :s 0 :dw 4))
             (sq "SELECT EXTRACT(YEAR FROM at) AS y, EXTRACT(MONTH FROM at) AS m, EXTRACT(DAY FROM at) AS d,
                         EXTRACT(HOUR FROM at) AS h, EXTRACT(MINUTE FROM at) AS mi, EXTRACT(SECOND FROM at) AS s,
                         EXTRACT(DOW FROM at) AS dw FROM events WHERE what = 'leap'")))
  (is (equal (list (ut "2024-03-05"))
             (mapcar (lambda (r) (getf r :v)) (sq "SELECT date('2024-03-05') AS v FROM events LIMIT 1"))))
  (is (equal (list (+ (ut "2024-01-01") 86400))
             (mapcar (lambda (r) (getf r :v)) (sq "SELECT day + 86400 AS v FROM events WHERE what = 'new year'")))
      "to add time, add seconds"))

(def-test sql-current-date (:fixture clean-db)
  (make-sql-events)
  (with-clock ((ut "2024-02-29 20:00:00"))
    (is (equal '("leap") (mapcar (lambda (r) (getf r :what)) (sq "SELECT what FROM events WHERE day = CURRENT_DATE"))))
    (is (equal '("new year" "lunch" "leap")
               (mapcar (lambda (r) (getf r :what))
                       (sq "SELECT what FROM events WHERE day <= current_date ORDER BY id"))))
    (is (equal '((:v "2024-02-29")) (sq "SELECT date_text(CURRENT_DATE) AS v FROM events LIMIT 1")))
    (is (equal '((:v "2024-02-29 20:00:00")) (sq "SELECT timestamp_text(now()) AS v FROM events LIMIT 1")))))

(def-test sql-dates-in-group-order-and-null (:fixture clean-db)
  (make-sql-events)
  (is (equal '((:m 1 :n 2) (:m 2 :n 1) (:m 7 :n 1) (:m :null :n 1))
             (sq "SELECT month(day) AS m, count(*) AS n FROM events GROUP BY month(day) ORDER BY m")))
  (is (equal '("unknown")
             (mapcar (lambda (r) (getf r :what)) (sq "SELECT what FROM events WHERE year(at) IS NULL"))))
  (is (equal '("summer" "leap" "lunch" "new year" "unknown")
             (mapcar (lambda (r) (getf r :what)) (sq "SELECT what FROM events ORDER BY at DESC")))))

(def-test sql-dates-use-an-index (:fixture clean-db)
  (make-sql-events)
  (sql "CREATE INDEX ON events (at)")
  (let ((steps (sql "EXPLAIN SELECT what FROM events WHERE at >= TIMESTAMP '2024-02-01 00:00:00'")))
    (is (equal '(:index :range 2) (list (getf (cddr (first steps)) :access) (getf (cddr (first steps)) :op)
                                        (getf (cddr (first steps)) :rows))))))

(def-test sql-date-column-types (:fixture clean-db)
  (sql "CREATE TABLE t (a DATE, b DATETIME, c TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
  (is (equal '((:a timestamp) (:b timestamp) (:c timestamp)) (column-summary "t"))))

(def-test sql-date-errors (:fixture clean-db)
  (make-sql-events)
  (signals query-error (sql "SELECT date('2024-02-30') FROM events"))
  (signals query-error (sql "SELECT what FROM events WHERE day = DATE 'soon'"))
  (signals query-error (sql "SELECT year(what) FROM events"))
  (flet ((fails (text)
           (handler-case (progn (sql text) nil)
             (sql-error (c) (error-message c)))))
    (is (search "EXTRACT can take" (fails "SELECT EXTRACT(WEEK FROM at) FROM events")))
    (is (search "Expected FROM" (fails "SELECT EXTRACT(YEAR at) FROM events")))
    (is (search "takes 1 argument" (fails "SELECT year() FROM events")))
    (is (search "takes 0 argument" (fails "SELECT today(1) FROM events")))))

(def-test a-column-called-date-still-works (:fixture clean-db)
  (sql "CREATE TABLE t (id INTEGER PRIMARY KEY, \"date\" TEXT, timestamp TEXT)")
  (sql "INSERT INTO t (\"date\", timestamp) VALUES ('a', 'b')")
  (is (equal '((:date "a" :timestamp "b")) (sq "SELECT \"date\", timestamp FROM t"))))
