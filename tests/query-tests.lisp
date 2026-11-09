(in-package #:hash-db-tests)

(in-suite hash-db-suite)

;;; Helpers. make-people, add-people, names-of and count-rows come from tests.lisp.
;;; People: 1 ann (30, active), 2 bob (25, not active), 3 cat (30, active).

(defun make-sample-people ()
  "People with a spread of values, including NULL ages and names."
  (make-people)
  (add-people)
  (insert-row '(:name "dan") "people")                      ; 4: age and active are NULL
  (insert-row '(:name "Eve" :age 41 :active t) "people"))   ; 5

(defun names-where (where)
  "Names of the people that pass WHERE, sorted without regard to case."
  (sort (names-of (select :from "people" :where where)) #'string-lessp))

;;; :where as expression, function and function name

(defun is-thirty (row) (eql 30 (getf row :age)))

(def-test where-takes-a-lambda-a-function-name-and-an-expression (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "cat") (names-where (lambda (r) (eql 30 (getf r :age))))))
  (is (equal '("ann" "cat") (names-where 'is-thirty)))
  (is (equal '("ann" "cat") (names-where #'is-thirty)))
  (is (equal '("ann" "cat") (names-where '(= :age 30))))
  (is (equal '("ann" "cat") (names-where (matching "people" :age 30)))))

(def-test where-t-keeps-every-row-and-a-boolean-column-works (:fixture clean-db)
  (make-sample-people)
  (is (= 5 (length (names-where t))))
  (is (equal '("ann" "cat" "Eve") (names-where :active))))

;;; comparison and NULL

(def-test comparisons (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "cat" "Eve") (names-of (select :from "people" :where '(> :age 25) :order-by :id))))
  (is (equal '("ann" "bob" "cat") (names-where '(<= :age 30))))
  (is (equal '("Eve") (names-where '(>= :age 31))))
  (is (equal '("bob" "Eve") (names-where '(/= :age 30)))
      "/= with NULL is unknown, so dan is out"))

(def-test null-gives-unknown-and-where-keeps-only-true (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "bob" "cat" "Eve") (names-where '(or (< :age 100) (> :age 0)))))
  (is (equal '("bob" "Eve") (names-where '(not (= :age 30))))
      "NOT unknown is unknown, so dan is out")
  (is (equal '("dan") (names-where '(is-null :age))))
  (is (equal '("ann" "bob" "cat" "Eve") (names-where '(is-not-null :age))))
  (is (equal '("ann" "cat" "dan") (names-where '(or (= :age 30) (is-null :age))))))

(def-test and-or-three-valued-logic (:fixture clean-db)
  (make-sample-people)
  ;; dan: age NULL, active NULL
  (is (equal '() (names-where '(and (= :age 30) :active (= :name "dan")))))
  (is (equal '("dan") (names-where '(or (= :name "dan") (= :age 99)))) "true or unknown is true")
  (is (equal '() (names-where '(and (= :name "dan") (= :age 30)))) "true and unknown is unknown")
  (is (equal '("bob") (names-where '(and (= :age 25) (not :active)))) "false and ... is false"))

(def-test in-between-and-like (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "bob" "cat") (names-where '(in :age 25 30))))
  (is (equal '("ann" "bob" "cat") (names-where '(in :name "ann" "bob" "cat" "zed"))))
  (is (equal '("ann" "bob" "cat") (names-where '(between :age 25 30))))
  (is (equal '("ann") (names-where '(like :name "a%"))))
  (is (equal '("cat" "dan") (names-where '(like :name "_a_"))) "a in the middle")
  (is (equal '("Eve") (names-where '(like :name "E%e"))))
  (is (equal '("ann" "bob" "cat" "dan" "Eve") (names-where '(like :name "%"))))
  (is (equal '("ann") (names-where '(like :name "ann")))))

(def-test like-escapes-and-null (:fixture clean-db)
  (make-table "t" '((:s string)))
  (dolist (s '("100%" "100x" "a_b" "axb" "back\\slash"))
    (insert-row (list :s s) "t"))
  (insert-row '() "t")
  (flet ((hits (pattern)
           (mapcar (lambda (r) (getf r :s))
                   (row-list (select :from "t" :where `(like :s ,pattern) :order-by :s)))))
    (is (equal '("100%") (hits "100\\%")))
    (is (equal '("a_b") (hits "a\\_b")))
    (is (equal '("a_b" "axb") (hits "a_b")))
    (is (equal '("back\\slash") (hits "back\\\\slash")))))

(def-test operator-names-match-by-name (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann") (names-where '(:like :name "a%"))))
  (is (equal '("ann") (names-where '(LIKE :name "a%")))))

;;; arithmetic, strings and functions

(def-test arithmetic-and-strings (:fixture clean-db)
  (make-sample-people)
  (is (equal '("bob") (names-where '(= (+ :age 5) 30))))
  (is (equal '("ann" "cat") (names-where '(= (* :age 2) 60))))
  (is (equal '("Eve") (names-where '(= (- :age 1) 40))))
  (is (equal '("ann" "cat") (names-where '(= (/ :age 2) 15))))
  (is (equal '("Eve") (names-where '(= (lower :name) "eve"))))
  (is (equal '("Eve") (names-where '(= (upper :name) "EVE"))))
  (is (equal '("ann" "bob" "cat" "dan" "Eve") (names-where '(= (length :name) 3))))
  (is (equal '("ann") (names-where '(= (concat :name "!") "ann!"))))
  (is (equal '("dan") (names-where '(= (coalesce :age 0) 0)))))

(def-test null-in-arithmetic-gives-null (:fixture clean-db)
  (make-sample-people)
  (is (equal '() (names-where '(= (+ :age 1) 1))) "dan's NULL age stays NULL")
  (is (equal '("dan") (names-where '(is-null (+ :age 1))))))

(def-test division-by-zero-is-a-query-error (:fixture clean-db)
  (make-sample-people)
  (signals query-error (select :from "people" :where '(= (/ :age 0) 1))))

(defun twice (n) (* 2 n))
(defun starts-with-a (s) (and (stringp s) (plusp (length s)) (char= #\a (char s 0))))

(def-test fn-calls-your-function (:fixture clean-db)
  (make-sample-people)
  ;; fn passes NULL to your function as :null, so guard it. AND stops at the first false.
  (is (equal '("ann" "cat") (names-where '(and (is-not-null :age) (= (fn twice :age) 60)))))
  (is (equal '("ann") (names-where '(fn starts-with-a :name))))
  (is (equal '("ann") (names-where (list 'fn #'starts-with-a :name))))
  (is (equal '("ann") (names-where `(fn ,(lambda (s) (string= s "ann")) :name)))))

(def-test quote-makes-a-constant-keyword (:fixture clean-db)
  (make-table "t" '((:id primary-key-column) (:color keyword)))
  (insert-row '(:color :red) "t")
  (insert-row '(:color :blue) "t")
  (is (= 1 (count-rows (select :from "t" :where '(= :color (quote :red)))))))

;;; errors

(def-test expression-errors-are-query-errors (:fixture clean-db)
  (make-people)
  (signals no-such-column (select :from "people" :where '(= :nope 1)))
  (signals query-error (select :from "people" :where '(frobnicate :age)))
  (signals query-error (select :from "people" :where '(= :age)))
  (signals query-error (select :from "people" :where '(= :age 1 2)))
  (signals query-error (select :from "people" :where '(= age 1)))
  (signals query-error (select :from "people" :where '((lambda (x) x) 1)))
  (signals query-error (select :from "people" :where 'no-such-function))
  (signals query-error (select :from "people" :where '(fn no-such-function :age))))

(def-test expression-errors-show-even-on-an-empty-table (:fixture clean-db)
  (make-people)
  (signals no-such-column (select :from "people" :where '(= :nope 1))))

(def-test incomparable-values-are-a-query-error (:fixture clean-db)
  (make-sample-people)
  (signals query-error (select :from "people" :where '(< :name 5)))
  (signals query-error (select :from "people" :where '(like :age "3%")))
  (signals query-error (select :from "people" :where '(= (+ :name 1) 1))))

(def-test expressions-work-on-joins-and-select-results (:fixture clean-db)
  (make-sample-people)
  (make-table "pets" '((:pet-id primary-key-column)
                       (:owner foreign-key-column :references "people")
                       (:pet string)))
  (insert-row '(:owner 1 :pet "rex") "pets")
  (insert-row '(:owner 2 :pet "tom") "pets")
  (is (equal '("rex") (mapcar (lambda (r) (getf r :pet))
                              (row-list (select :from "pets"
                                                :inner-join '("people" :owner :id)
                                                :where '(> :age 26))))))
  (let ((old (select :from "people" :where '(>= :age 30))))
    (is (equal '("ann") (names-of (select :from old :where '(like :name "a%")))))))

;;; limit and offset

(defun ids-of (table) (mapcar (lambda (r) (getf r :id)) (row-list table)))

(def-test limit-and-offset (:fixture clean-db)
  (make-sample-people)
  (flet ((ids (&rest args) (ids-of (apply #'select :from "people" :order-by :id args))))
    (is (equal '(1 2) (ids :limit 2)))
    (is (equal '(3 4 5) (ids :offset 2)))
    (is (equal '(2 3) (ids :limit 2 :offset 1)))
    (is (equal '(5) (ids :limit 10 :offset 4)))
    (is (equal '() (ids :limit 0)))
    (is (equal '() (ids :offset 99)))
    (is (equal '(1 2 3 4 5) (ids :limit 99)))))

(def-test limit-applies-after-where-sort-and-distinct (:fixture clean-db)
  (make-sample-people)
  (is (equal '(5 1) (ids-of (select :from "people" :where '(is-not-null :age)
                                    :order-by '((desc :age) :id) :limit 2))))
  (is (equal '((:age 25) (:age 30))
             (row-list (select :from "people" :col :age :distinct t :order-by :age :limit 2)))))

(def-test bad-limit-and-offset (:fixture clean-db)
  (make-people)
  (signals query-error (select :from "people" :limit -1))
  (signals query-error (select :from "people" :limit 1.5))
  (signals query-error (select :from "people" :offset "2")))

;;; ordering

(defun order-names (order-by)
  (names-of (select :from "people" :order-by order-by)))

(def-test descending-order (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve" "ann" "cat" "bob" "dan") (order-names '((desc :age) :id))))
  (is (equal '("Eve" "ann" "cat" "bob" "dan") (order-names '((desc :age) :id))))
  (is (equal '("Eve" "ann" "cat" "bob" "dan") (order-names '((:desc :age) :id))))
  (is (equal '("dan" "cat" "bob" "ann" "Eve") (order-names '(desc :name)))
      "strings sort by character code, so the capital E comes last when reversed"))

(def-test nulls-go-last-unless-told-otherwise (:fixture clean-db)
  (make-sample-people)
  (is (equal '("bob" "ann" "cat" "Eve" "dan") (order-names '(:age :id))))
  (is (equal "dan" (car (last (order-names '((desc :age) :id))))) "last in descending too")
  (is (equal "dan" (first (order-names '((asc :age :nulls :first) :id)))))
  (is (equal "dan" (first (order-names '((desc :age :nulls :first) :id))))))

(def-test several-keys-with-mixed-directions (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve" "ann" "cat" "bob" "dan")
             (order-names '((asc (- 0 :age) :nulls :last) :id)))
      "an expression key")
  (is (equal '("Eve" "cat" "bob" "ann" "dan") (order-names '((asc (is-null :age)) (desc :id))))
      "keys can be booleans too: false before true"))

(def-test expression-keys (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve" "ann" "cat" "bob" "dan")
             (order-names '((desc (+ :age 0)) :id))))
  (is (equal '("ann" "bob" "cat" "dan" "Eve")
             (order-names '((lower :name))))))

(def-test sort-is-stable (:fixture clean-db)
  (make-sample-people)
  (is (equal '("ann" "cat") (subseq (order-names '((desc :age) :active)) 1 3))
      "ann and cat tie on both keys, so they keep their id order"))

(def-test order-by-a-lambda (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve" "dan" "cat" "bob" "ann")
             (names-of (select :from "people"
                               :order-by (lambda (a b) (> (getf a :id) (getf b :id))))))))

(def-test column-named-desc-needs-a-wrapper (:fixture clean-db)
  (make-table "t" '((:id primary-key-column) (:desc string)))
  (insert-row '(:desc "b") "t")
  (insert-row '(:desc "a") "t")
  (is (equal '("a" "b") (mapcar (lambda (r) (getf r :desc))
                                (row-list (select :from "t" :order-by '((asc :desc))))))))

(def-test bad-order-keys (:fixture clean-db)
  (make-sample-people)
  (signals no-such-column (select :from "people" :order-by :nope))
  (signals no-such-column (select :from "people" :order-by '(desc :nope)))
  (signals query-error (select :from "people" :order-by '(asc :age :nulls :middle)))
  (signals query-error (select :from "people" :order-by '((frobnicate :age)))))

(def-test any-column-sorts-through-expressions (:fixture clean-db)
  (make-table "t" '((:x any)))
  (insert-row '(:x 3) "t")
  (insert-row '(:x 1) "t")
  (is (equal '(1 3) (mapcar (lambda (r) (getf r :x))
                            (row-list (select :from "t" :order-by :x))))))

;;; computed columns and aliases

(defun rows-of (&rest args)
  (row-list (apply #'select :from "people" args)))

(def-test computed-columns-and-aliases (:fixture clean-db)
  (make-sample-people)
  (is (equal '((:name "ann" :next-age 31) (:name "bob" :next-age 26))
             (rows-of :col '(:name (as (+ :age 1) :next-age)) :where '(<= :age 30) :order-by :id :limit 2))))

(def-test unnamed-expressions-are-named-by-position (:fixture clean-db)
  (make-sample-people)
  (is (equal '((:name "ann" :expr2 "ANN" :expr3 60))
             (rows-of :col '(:name (upper :name) (* :age 2)) :where '(= :id 1))
             ) "positions count from 1 and include plain columns")
  (is (equal '(:expr1) (mapcar #'name (schema (select :from "people" :col '((+ :age 1))))))))

(def-test alias-renames-a-column-and-keeps-its-type (:fixture clean-db)
  (make-sample-people)
  (let ((result (select :from "people" :col '((as :age :years)) :order-by :id)))
    (is (equal '(:years) (mapcar #'name (schema result))))
    (is (equal '(30 25 30 :null 41) (mapcar (lambda (r) (getf r :years)) (row-list result))))
    (is (equal '("bob") (names-of (select :from "people" :where '(= :age 25))))))
  (is (equal '(:years 25)
             (first (row-list (select :from (select :from "people" :col '((as :age :years)))
                                      :where '(= :years 25)))))
      "the result table can be queried, and its renamed column keeps working"))

(def-test aliases-work-in-order-by (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve" "ann" "cat" "bob")
             (mapcar (lambda (r) (getf r :name))
                     (rows-of :col '(:name (as (- 0 :age) :neg)) :where '(is-not-null :age)
                              :order-by '(:neg :id)))))
  (is (equal '("Eve" "ann" "cat" "bob")
             (mapcar (lambda (r) (getf r :name))
                     (rows-of :col '(:name (as (+ :age 0) :a)) :where '(is-not-null :age)
                              :order-by '((desc :a)))))))

(def-test alias-wins-over-a-column-of-the-same-name-in-order-by (:fixture clean-db)
  (make-sample-people)
  (is (equal '("Eve" "ann" "cat" "bob")
             (mapcar (lambda (r) (getf r :name))
                     (rows-of :col '(:name (as (- 0 :age) :age)) :where '(is-not-null :age)
                              :order-by '(:age :id))))))

(def-test where-cannot-use-aliases (:fixture clean-db)
  (make-sample-people)
  (signals no-such-column (rows-of :col '(:name (as (+ :age 1) :next)) :where '(> :next 30))))

(def-test computed-columns-with-null (:fixture clean-db)
  (make-sample-people)
  (is (eq :null (getf (first (rows-of :col '(:id (as (+ :age 1) :n)) :where '(= :id 4))) :n)))
  (is (equal '(:n :null) (first (rows-of :col '((as :null :n)) :where '(= :id 1)))))
  (is (equal '(:c 7) (first (rows-of :col '((as (coalesce :age 7) :c)) :where '(= :id 4))))))

(def-test distinct-works-on-computed-columns (:fixture clean-db)
  (make-sample-people)
  (is (equal '((:null-age nil :old t) (:null-age nil :old nil) (:null-age t :old :null))
             (rows-of :col '((as (is-null :age) :null-age) (as (> :age 29) :old))
                      :distinct t :order-by :id))
      "ann and cat give one pair. Unknown shows as NULL."))
(def-test unknown-becomes-null-when-stored-or-passed-on (:fixture clean-db)
  (make-sample-people)
  (is (equal '(:null) (mapcar (lambda (r) (getf r :v))
                              (rows-of :col '((as (> :age 1) :v)) :where '(= :id 4)))))
  (is (equal '(t) (mapcar (lambda (r) (getf r :v))
                          (rows-of :col '((as (is-null (> :age 1)) :v)) :where '(= :id 4)))))
  (is (equal '(7) (mapcar (lambda (r) (getf r :v))
                          (rows-of :col '((as (coalesce (> :age 1) 7) :v)) :where '(= :id 4)))))
  (is (equal '("dan") (names-where '(fn is-null-value (> :age 1))))))

(defun is-null-value (v) (eq v :null))

(def-test result-column-errors (:fixture clean-db)
  (make-sample-people)
  (signals no-such-column (rows-of :col :nope))
  (signals no-such-column (rows-of :col '((as :nope :x))))
  (signals query-error (rows-of :col '(:name (as :age :name))))
  (signals query-error (rows-of :col '((as :age))))
  (signals query-error (rows-of :col '((frobnicate :age))))
  (signals query-error (rows-of :col nil :where '(= :id 1)) "an empty list of items is not t"))

(def-test column-names-still-work-as-before (:fixture clean-db)
  (make-sample-people)
  (is (equal '((:name "ann" :age 30)) (rows-of :col '(:name :age) :where '(= :id 1))))
  (is (equal '((:name "ann")) (rows-of :col :name :where '(= :id 1)))))
