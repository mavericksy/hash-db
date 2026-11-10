(in-package #:hash-db-tests)

(in-suite hash-db-suite)

;;; EXPLAIN

(def-test sql-explain-shows-the-plan (:fixture clean-db)
  (make-sample-people)
  (create-index "people" :age)
  (let ((steps (sql "EXPLAIN SELECT name FROM people WHERE age = 30 AND name LIKE 'a%' ORDER BY name LIMIT 5")))
    (is (equal '(:scan :filter :sort :project :limit) (mapcar #'second steps)))
    (is (equal '(:index :age-index :eq 2)
               (let ((scan (cddr (first steps))))
                 (list (getf scan :access) (getf scan :index) (getf scan :op) (getf scan :rows))))))
  (is (equal :full-scan (getf (cddr (first (sql "EXPLAIN SELECT * FROM people WHERE name = 'x'"))) :access))))

(def-test sql-explain-shows-joins (:fixture clean-db)
  (make-pet-world)
  (dotimes (i 40) (insert-row (list :owner 1 :pet "filler") "pets"))
  (let* ((steps (sql "EXPLAIN SELECT p.name, pt.pet FROM people p JOIN pets pt ON p.id = pt.owner WHERE p.id = 1"))
         (join (find :join steps :key #'second)))
    (is (equal '(:inner "pets" :index) (list (getf (cddr join) :kind) (getf (cddr join) :table) (getf (cddr join) :method))))))

(def-test sql-explain-does-not-run-the-query (:fixture clean-db)
  (make-sample-people)
  (let ((before (current-seq)))
    (sql "EXPLAIN SELECT * FROM people")
    (is (= before (current-seq))))
  (signals sql-error (sql "EXPLAIN INSERT INTO people (name) VALUES ('x')"))
  (signals sql-error (sql "EXPLAIN UPDATE people SET age = 1"))
  (is (= 5 (count-rows "people"))))

;;; sql-script

(def-test sql-script-runs-statements-in-order (:fixture clean-db)
  (let ((results (sql-script "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
                              INSERT INTO people (name, age) VALUES ('ann', 30), ('bob', 25);
                              UPDATE people SET age = age + 1;
                              DELETE FROM people WHERE name = 'bob';
                              SELECT name, age FROM people;")))
    (is (= 5 (length results)))
    (is (equal "people" (first results)))
    (is (= 2 (second results)))
    (is (= 2 (third results)))
    (is (= 1 (fourth results)))
    (is (equal '((:name "ann" :age 31)) (row-list (fifth results))))))

(def-test sql-script-parameters-run-across-statements (:fixture clean-db)
  (sql-script "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER);
               INSERT INTO t (a, b) VALUES (?, ?);
               INSERT INTO t (a, b) VALUES (?, ?);
               UPDATE t SET b = ? WHERE a = ?"
              "x" 1 "y" 2 99 "y")
  (is (equal '((:id 1 :a "x" :b 1) (:id 2 :a "y" :b 99)) (row-list "t")))
  (signals sql-error (sql-script "SELECT * FROM t WHERE a = ?; SELECT * FROM t WHERE b = ?" "x"))
  (signals sql-error (sql-script "SELECT * FROM t" 1)))

(def-test sql-script-is-all-or-nothing (:fixture clean-db)
  (sql "CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT UNIQUE)")
  (signals unique-violation
    (sql-script "INSERT INTO t (u) VALUES ('a');
                 CREATE TABLE extra (x INTEGER);
                 INSERT INTO t (u) VALUES ('b');
                 INSERT INTO t (u) VALUES ('a')"))
  (is (= 0 (count-rows "t")))
  (is (equal '("t") (show-tables)) "the table made in the script is gone too")
  (is (= 1 (insert-row '(:u "a") "t")) "the ids came back"))

(def-test sql-script-edge-cases (:fixture clean-db)
  (sql "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
  (is (equal '((1 1)) (list (multiple-value-list (sql ";;; INSERT INTO t (a) VALUES (1)")))) "sql skips empty statements around one")
  (is (equal '(1) (sql-script ";;; INSERT INTO t (a) VALUES (2) ;; ;")) "a script gives one result for each statement")
  (signals sql-error (sql-script "   "))
  (signals sql-error (sql-script "INSERT INTO t (a) VALUES (1) garbage"))
  (signals sql-error (sql-script "INSERT INTO t (a) VALUES (1); FROBNICATE"))
  (is (= 2 (count-rows "t")) "a script that does not parse does not run at all"))

(def-test sql-script-inside-a-transaction (:fixture clean-db)
  (sql "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)")
  (with-transaction
    (sql-script "INSERT INTO t (a) VALUES (1); INSERT INTO t (a) VALUES (2)")
    (is (= 2 (count-rows "t")))
    (rollback))
  (is (= 0 (count-rows "t"))))

(def-test sql-script-reaches-the-log (:fixture clean-db)
  (with-temp-dir (dir)
    (let ((path (merge-pathnames "t.db" dir)))
      (open-db path)
      (sql-script "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER CHECK (a > 0));
                   CREATE INDEX ON t (a);
                   INSERT INTO t (a) VALUES (5), (6)")
      (restart-db path)
      (is (= 2 (count-rows "t")))
      (is (equal '(:a-index) (index-names "t")))
      (signals check-violation (sql "INSERT INTO t (a) VALUES (0)")))))

;;; a worked example, as in the README

(def-test sql-worked-example (:fixture clean-db)
  (sql-script "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, city TEXT);
               CREATE TABLE orders (id INTEGER PRIMARY KEY,
                                    customer INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                                    total NUMERIC CHECK (total >= 0));
               CREATE INDEX ON orders (total);
               INSERT INTO customers (name, city) VALUES ('ann', 'oslo'), ('bob', 'rome'), ('cat', 'oslo');
               INSERT INTO orders (customer, total) VALUES (1, 10), (1, 25.5), (3, 7), (2, 100)")
  (is (equal '((:city "oslo" :orders 3 :revenue 42.5d0) (:city "rome" :orders 1 :revenue 100))
             (q "SELECT c.city, count(*) AS orders, sum(o.total) AS revenue
                 FROM customers c JOIN orders o ON c.id = o.customer
                 GROUP BY c.city ORDER BY c.city")))
  (is (equal '(("bob" 100) ("ann" 25.5d0))
             (pairs (q "SELECT c.name, max(o.total) AS biggest FROM customers c JOIN orders o ON c.id = o.customer
                        GROUP BY c.name HAVING max(o.total) > ? ORDER BY biggest DESC" 20)
                    :name :biggest)))
  (is (= 1 (sql "DELETE FROM customers WHERE name = 'ann'")))
  (is (= 2 (count-rows "orders")) "the orders of ann went with her"))
