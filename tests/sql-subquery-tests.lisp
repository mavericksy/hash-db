(in-package #:hash-db-tests)

(in-suite hash-db-suite)

;;; scalar subqueries

(def-test sql-scalar-subquery (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve") (mapcar (lambda (r) (getf r :name))
                              (sq "SELECT name FROM people WHERE age = (SELECT max(age) FROM people)"))))
  (is (equal '("ann" "cat" "Eve") (mapcar (lambda (r) (getf r :name))
                                          (sq "SELECT name FROM people WHERE age >= (SELECT min(age) + 5 FROM people) ORDER BY id"))))
  (is (equal '((:oldest 41)) (sq "SELECT (SELECT max(age) FROM people) AS oldest FROM people LIMIT 1")))
  (is (equal '((:v :null)) (sq "SELECT (SELECT age FROM people WHERE name = 'nobody') AS v FROM people LIMIT 1"))
      "no row gives NULL")
  (is (equal '((:v "ann")) (sq "SELECT (SELECT name FROM people WHERE id = 1) AS v FROM people LIMIT 1")))
  (is (equal '(("bob")) (mapcar (lambda (r) (list (getf r :name)))
                                (sq "SELECT name FROM people WHERE age < (SELECT avg(age) FROM people WHERE age IS NOT NULL) AND age = (SELECT min(age) FROM people)"))))
  (is (equal '((:v 41)) (sq "SELECT (SELECT m FROM (SELECT max(age) AS m FROM people) x) AS v FROM people LIMIT 1")) "nested"))

(def-test sql-scalar-subquery-errors (:fixture clean-db)
  (make-sample-people)
  (flet ((fails (text)
           (handler-case (progn (sql text) nil)
             (sql-error (c) (error-message c)))))
    (is (search "one column, and this gives 2" (fails "SELECT name FROM people WHERE age = (SELECT age, name FROM people LIMIT 1)")))
    (is (search "more than one row" (fails "SELECT name FROM people WHERE age = (SELECT age FROM people)")))
    (is (search "Unknown column nope" (fails "SELECT name FROM people WHERE age = (SELECT nope FROM people)")))))

;;; IN and EXISTS

(def-test sql-in-subquery (:fixture clean-db)
  (make-pet-world)
  (is (equal '("ann" "cat") (mapcar (lambda (r) (getf r :name))
                                    (sq "SELECT name FROM people WHERE id IN (SELECT owner FROM pets) ORDER BY id"))))
  (is (equal '("bob") (mapcar (lambda (r) (getf r :name))
                              (sq "SELECT name FROM people WHERE id NOT IN (SELECT owner FROM pets WHERE owner IS NOT NULL) ORDER BY id"))))
  (is (equal '() (sq "SELECT name FROM people WHERE id IN (SELECT owner FROM pets WHERE pet = 'nobody')"))
      "an empty answer")
  (is (= 3 (length (sq "SELECT name FROM people WHERE id NOT IN (SELECT owner FROM pets WHERE pet = 'nobody')")))
      "NOT IN an empty answer keeps everything")
  (is (equal '("ann") (mapcar (lambda (r) (getf r :name))
                              (sq "SELECT name FROM people WHERE id IN (SELECT owner FROM pets WHERE pet LIKE ?) AND age = ?" "t%" 30)))))

(def-test sql-not-in-with-a-null-in-the-answer (:fixture clean-db)
  (make-pet-world)
  ;; the stray pet has a NULL owner, so NOT IN is never true
  (is (equal '() (sq "SELECT name FROM people WHERE id NOT IN (SELECT owner FROM pets)")))
  (is (equal '("ann" "cat") (mapcar (lambda (r) (getf r :name))
                                    (sq "SELECT name FROM people WHERE id IN (SELECT owner FROM pets) ORDER BY id")))))

(def-test sql-exists (:fixture clean-db)
  (make-pet-world)
  (is (= 3 (length (sq "SELECT name FROM people WHERE EXISTS (SELECT 1 FROM pets)")))
      "an uncorrelated EXISTS is the same for every row")
  (is (equal '() (sq "SELECT name FROM people WHERE EXISTS (SELECT pet_id FROM pets WHERE pet = 'nobody')")))
  (is (= 3 (length (sq "SELECT name FROM people WHERE NOT EXISTS (SELECT pet_id FROM pets WHERE pet = 'nobody')"))))
  (is (equal '() (sq "SELECT name FROM people WHERE NOT EXISTS (SELECT pet_id FROM pets)"))))

(def-test sql-subquery-answer-is-data (:fixture clean-db)
  (make-table "words" '((:id primary-key-column) (:w string)))
  (sql "INSERT INTO words (w) VALUES ('x'' OR ''1''=''1'), ('plain')")
  (sql "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)")
  (sql "INSERT INTO t (s) VALUES ('x'' OR ''1''=''1'), ('plain'), ('other')")
  (is (equal '("x' OR '1'='1") (mapcar (lambda (r) (getf r :s))
                                       (sq "SELECT s FROM t WHERE s = (SELECT w FROM words WHERE id = 1)"))))
  (is (equal '("x' OR '1'='1" "plain") (mapcar (lambda (r) (getf r :s))
                                                (sq "SELECT s FROM t WHERE s IN (SELECT w FROM words) ORDER BY id")))))

(def-test sql-subquery-in-other-statements (:fixture clean-db)
  (make-three-people)
  (sql "CREATE TABLE archive (id INTEGER PRIMARY KEY, name TEXT)")
  (is (= 1 (sql "INSERT INTO archive (name) VALUES ((SELECT name FROM people WHERE age = 25))")))
  (is (equal '("bob") (mapcar (lambda (r) (getf r :name)) (row-list "archive"))))
  (is (= 2 (sql "UPDATE people SET note = (SELECT name FROM people WHERE id = 1) WHERE age = 30")))
  (is (equal '("ann" "ann") (mapcar (lambda (r) (getf r :note)) (row-list (select :from "people" :where '(= :age 30) :order-by :id)))))
  (is (= 2 (sql "DELETE FROM people WHERE age IN (SELECT age FROM people WHERE name = 'ann')")))
  (is (equal '("bob") (mapcar (lambda (r) (getf r :name)) (row-list "people")))))

(def-test sql-subquery-uses-indexes-on-the-outer-table (:fixture clean-db)
  (make-sample-people)
  (create-index "people" :age)
  (let ((steps (sql "EXPLAIN SELECT name FROM people WHERE age = (SELECT max(age) FROM people)")))
    (is (equal '(:index :eq 1) (list (getf (cddr (first steps)) :access) (getf (cddr (first steps)) :op)
                                     (getf (cddr (first steps)) :rows))))))

;;; derived tables

(def-test sql-derived-table (:fixture clean-db)
  (make-sales)
  (is (equal '((:dept "toys" :total 60) (:dept "food" :total 20))
             (sq "SELECT dept, total FROM (SELECT dept, sum(amount) AS total FROM sales GROUP BY dept) AS t
                  WHERE total >= 20 ORDER BY total DESC")))
  (is (equal '((:n 5)) (sq "SELECT count(*) AS n FROM (SELECT dept FROM sales WHERE dept IS NOT NULL) d WHERE d.dept <> 'misc'"))
      "an alias without AS")
  (is (equal '((:avg-total 40.0d0)) (mapcar (lambda (r) (list :avg-total (getf r :avg-total)))
                                              (sq "SELECT avg(total) FROM (SELECT dept, sum(amount) AS total FROM sales WHERE amount IS NOT NULL GROUP BY dept) x WHERE dept IS NOT NULL"))))
  (is (equal '(("toys" 3)) (mapcar (lambda (r) (list (getf r :dept) (getf r :n)))
                                   (sq "SELECT dept, n FROM (SELECT dept, count(*) AS n FROM sales GROUP BY dept) q WHERE n = (SELECT max(n) FROM (SELECT count(*) AS n FROM sales GROUP BY dept) m)")))))

(def-test sql-derived-table-in-a-join (:fixture clean-db)
  (make-pet-world)
  (is (equal '(("ann" 2) ("cat" 1))
             (pairs (sq "SELECT p.name, c.n FROM people p
                         JOIN (SELECT owner, count(*) AS n FROM pets GROUP BY owner) c ON p.id = c.owner
                         ORDER BY p.name")
                    :name :n)))
  (is (equal '(("ann" 2) ("bob" :null) ("cat" 1))
             (pairs (sq "SELECT p.name, c.n FROM people p
                         LEFT JOIN (SELECT owner, count(*) AS n FROM pets GROUP BY owner) AS c ON p.id = c.owner
                         ORDER BY p.name")
                    :name :n))))

(def-test sql-derived-table-needs-an-alias (:fixture clean-db)
  (make-sample-people)
  (flet ((fails (text)
           (handler-case (progn (sql text) nil)
             (sql-error (c) (error-message c)))))
    (is (search "needs an alias" (fails "SELECT * FROM (SELECT id FROM people)")))
    (is (search "needs an alias" (fails "SELECT * FROM people JOIN (SELECT id FROM people) ON 1 = 1")))))

;;; the limits

(def-test sql-correlated-subqueries-are-refused-with-a-reason (:fixture clean-db)
  (make-pet-world)
  (flet ((fails (text)
           (handler-case (progn (sql text) nil)
             (sql-error (c) (error-message c)))))
    (is (search "belongs to the query around" (fails "SELECT name FROM people p WHERE EXISTS (SELECT 1 FROM pets WHERE owner = p.id)")))
    (is (search "belongs to the query around" (fails "SELECT name FROM people WHERE id = (SELECT max(owner) FROM pets WHERE owner = age)")))
    (is (search "belongs to the query around" (fails "SELECT name FROM people p WHERE age IN (SELECT age FROM people WHERE p.id = 1)")))))

(def-test sql-views-cannot-hold-subqueries (:fixture clean-db)
  (make-sample-people)
  (signals sql-error (sql "CREATE VIEW v AS SELECT name FROM people WHERE age = (SELECT max(age) FROM people)"))
  (signals sql-error (sql "CREATE VIEW v AS SELECT x.name FROM (SELECT name FROM people) x"))
  (is (null (show-views))))

(def-test sql-subquery-params-keep-text-order (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "cat") (mapcar (lambda (r) (getf r :name))
                                    (sq "SELECT name FROM people WHERE age = (SELECT max(age) FROM people WHERE age < ?) AND name <> ? ORDER BY id" 31 "zed"))))
  (is (equal '("cat") (mapcar (lambda (r) (getf r :name))
                              (sq "SELECT name FROM people WHERE name = ? AND age IN (SELECT age FROM people WHERE id = ?)" "cat" 3)))))
