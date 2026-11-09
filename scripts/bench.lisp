;;;; Time some queries with and without an index.
;;;; Run: sbcl --noinform --non-interactive --load scripts/bench.lisp
;;;; The numbers depend on the machine. Compare the two columns, not the totals.

(push (merge-pathnames "../" (uiop:pathname-directory-pathname *load-truename*))
      asdf:*central-registry*)
(asdf:load-system :hash-db)
(in-package :hash-db)

(defparameter *rows* 100000)

(defmacro timed ((label &optional (repeat 1)) &body body)
  `(let ((start (get-internal-real-time)))
     (dotimes (i ,repeat) ,@body)
     (format t "~&~42A ~8,3F ms each~%" ,label
             (/ (* 1000.0 (- (get-internal-real-time) start))
                internal-time-units-per-second ,repeat))))

(make-table "t" '((:id primary-key-column) (:k integer) (:u integer :unique t) (:s string)))
(format t "~&Loading ~D rows...~%" *rows*)
(with-transaction
  (dotimes (i *rows*)
    (insert-row (list :k (mod (* i 7919) 1000) :u i :s (format nil "s~6,'0D" (mod (* i 31) *rows*))) "t")))
(create-index "t" :s)

(format t "~&One value among ~D rows, about ~D rows match:~%" *rows* (floor *rows* 1000))
(timed ("= on a column without an index (scan)" 20)
  (select :from "t" :where '(= :k 500)))
(create-index "t" :k)
(timed ("= on the same column with an index" 20)
  (select :from "t" :where '(= :k 500)))
(timed ("= on the primary key" 200)
  (select :from "t" :where '(= :id 777)))
(timed ("= on a unique column" 200)
  (select :from "t" :where '(= :u 777)))
(format t "~&A range of 100 rows:~%")
(timed ("range, scan (lambda cannot use an index)" 5)
  (select :from "t" :where (lambda (r) (and (>= (getf r :u) 5000) (< (getf r :u) 5100)))))
(timed ("range, index on a string column" 20)
  (select :from "t" :where '(between :s "s005000" "s005099")))
(format t "~&Writes:~%")
(timed ("insert with a unique check" 2000)
  (insert-row (list :k 1 :u (+ *rows* (random 1000000000)) :s "x") "t"))
(timed ("delete-where on the primary key" 200)
  (delete-where "t" :where `(= :id ,(1+ (random 1000)))))
