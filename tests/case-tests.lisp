(in-package #:hash-db-tests)

(in-suite hash-db-suite)

;;; the expression

(defun case-names (case-expression)
  "The :v of every person, for the expression."
  (mapcar (lambda (r) (getf r :v))
          (row-list (select :from "people" :col `(:id (as ,case-expression :v)) :order-by :id))))

(def-test case-picks-the-first-true-condition (:fixture clean-db)
  (make-sample-people)
  (is (equal '("thirty" "young" "thirty" "none" "old")
             (case-names '(:case (= :age 30) "thirty" (< :age 30) "young" (is-null :age) "none" "old"))))
  (is (equal '("a" "a" "a" "a" "a") (case-names '(:case t "a" t "b"))) "the first true one wins"))

(def-test case-without-else-gives-null (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" :null "cat" :null :null)
             (case-names '(:case (and (= :age 30) :active) :name))) "an odd count has an else, an even one has not"))

(def-test case-treats-unknown-as-not-true (:fixture clean-db)
  (make-sample-people)
  ;; dan has NULL age, so (> age 20) is unknown and the else is used
  (is (equal '("old" "old" "old" "else" "old")
             (case-names '(:case (> :age 20) "old" "else")))))

(def-test case-works-out-only-the-branch-it-takes (:fixture clean-db)
  (make-sample-people)
  (is (equal '(:ok :ok :ok :ok :ok)
             (case-names '(:case (= 1 1) (quote :ok) (/ 1 0))))
      "the other branch has a division by zero and never runs")
  (signals query-error (case-names '(:case (= 1 1) (/ 1 0) 5))))

(def-test case-in-where-and-order (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "cat" "Eve") (names-where '(= (:case (>= :age 30) "x" "y") "x"))))
  (is (equal '("bob" "dan" "Eve" "ann" "cat")
             (mapcar (lambda (r) (getf r :name))
                     (row-list (select :from "people" :order-by '((:case (= :age 30) 1 0) :id)))))
      "a CASE as a sort key: the 30 year olds last"))

(def-test case-in-grouping (:fixture clean-db)
  (make-sales)
  (is (equal '((:size "big" :n 2) (:size "small" :n 4) (:size :null :n 1))
             (row-list (select :from "sales"
                               :group-by '((:case (>= :amount 20) "big" (is-not-null :amount) "small"))
                               :col '((as (:case (>= :amount 20) "big" (is-not-null :amount) "small") :size)
                                      (as (count) :n))
                               :order-by '((:case (= :size "big") 0 (= :size "small") 1 2))))))
  (is (equal '((:total 87 :flag "many"))
             (row-list (select :from "sales"
                               :col '((as (sum :amount) :total)
                                      (as (:case (> (count) 5) "many" "few") :flag)))))
      "an aggregate inside a CASE"))

(def-test case-needs-pairs (:fixture clean-db)
  (make-sample-people)
  (signals query-error (case-names '(:case)))
  (signals query-error (case-names '(:case 1))))

;;; SQL

(defun sq (text &rest params) (apply #'sql-rows text params))

(def-test sql-searched-case (:fixture clean-db)
  (make-sample-people)
  (is (equal '(("ann" "mid") ("bob" "young") ("cat" "mid") ("dan" "?") ("Eve" "old"))
             (mapcar (lambda (r) (list (getf r :name) (getf r :band)))
                     (sq "SELECT name, CASE WHEN age < 26 THEN 'young'
                                            WHEN age < 40 THEN 'mid'
                                            WHEN age IS NULL THEN '?'
                                            ELSE 'old' END AS band
                          FROM people ORDER BY id")))))

(def-test sql-simple-case (:fixture clean-db)
  (make-sample-people)
  (is (equal '("thirty" "25" "other" "other")
             (mapcar (lambda (r) (getf r :v))
                     (sq "SELECT CASE age WHEN 30 THEN 'thirty' WHEN 25 THEN '25' ELSE 'other' END AS v
                          FROM people WHERE id IN (1, 2, 4, 5) ORDER BY id"))))
  (is (equal '("thirty" :null :null)
             (mapcar (lambda (r) (getf r :v))
                     (sq "SELECT CASE age WHEN 30 THEN 'thirty' END AS v FROM people WHERE id IN (1, 2, 4) ORDER BY id")))
      "no ELSE gives NULL, and an unknown comparison with NULL is not a match"))

(def-test sql-case-everywhere (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "cat" "Eve")
             (mapcar (lambda (r) (getf r :name))
                     (sq "SELECT name FROM people WHERE CASE WHEN age >= 30 THEN 1 ELSE 0 END = 1 ORDER BY id"))))
  (is (equal '("bob" "ann" "cat" "Eve" "dan")
             (mapcar (lambda (r) (getf r :name))
                     (sq "SELECT name FROM people ORDER BY CASE WHEN age IS NULL THEN 1 ELSE 0 END, age, id"))))
  (is (equal '((:band "30+" :n 3) (:band "<30" :n 1) (:band :null :n 1))
             (sq "SELECT CASE WHEN age >= 30 THEN '30+' WHEN age < 30 THEN '<30' END AS band, count(*) AS n
                  FROM people GROUP BY CASE WHEN age >= 30 THEN '30+' WHEN age < 30 THEN '<30' END
                  ORDER BY n DESC, band"))
      "the same expression in the select list and in GROUP BY")
  (is (equal '((:v 5)) (sq "SELECT sum(CASE WHEN age = 30 THEN 1 ELSE 0 END) + 3 AS v FROM people")))
  (is (equal '((:v 2)) (sq "SELECT sum(CASE WHEN age = 30 THEN 1 ELSE 0 END) AS v FROM people")))
  (is (equal '((:v 1)) (sq "SELECT count(CASE WHEN age = 25 THEN 1 END) AS v FROM people"))
      "no ELSE gives NULL, and count skips NULL"))

(def-test sql-case-with-parameters-and-nesting (:fixture clean-db)
  (make-sample-people)
  (is (equal '("A" "B")
             (mapcar (lambda (r) (getf r :v))
                     (sq "SELECT CASE WHEN age = ? THEN CASE WHEN name = ? THEN 'A' ELSE 'B' END ELSE 'C' END AS v
                          FROM people WHERE id IN (1, 3) ORDER BY id" 30 "ann"))))
  (is (equal '("x") (mapcar (lambda (r) (getf r :v))
                            (sq "SELECT CASE WHEN ? THEN 'x' ELSE 'y' END AS v FROM people LIMIT 1" t)))))

(def-test sql-case-errors (:fixture clean-db)
  (make-sample-people)
  (flet ((fails (text)
           (handler-case (progn (sql text) nil)
             (sql-error (c) (error-message c)))))
    (is (search "Expected WHEN" (fails "SELECT CASE END FROM people")))
    (is (search "Expected WHEN" (fails "SELECT CASE age ELSE 1 END FROM people")))
    (is (search "Expected END" (fails "SELECT CASE WHEN age = 1 THEN 2 FROM people")))
    (is (search "Expected THEN" (fails "SELECT CASE WHEN age = 1 FROM people")))
    (is (search "Unknown column nope" (fails "SELECT CASE WHEN nope = 1 THEN 2 END FROM people")))))
