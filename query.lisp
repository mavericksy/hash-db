;;;; Queries: select and its helpers.

(in-package :hash-db)

;; Run a query.
;; WHERE is a function of one row (see MATCHING), the name of one, or an
;; expression (see expr.lisp).
;; INNER-JOIN is (table-name column-here column-there).
;; ORDER-BY is a column, an expression key, a list of keys, or a function of
;; two rows that is true when the first row sorts first. See COMPILE-ORDER-KEY.
;; LIMIT and OFFSET cut the result after sorting.
;; Return a new table. Its rows are keyed 1..n in result order.
(defun select (&key (col t) from where distinct order-by inner-join limit offset)
  (with-snapshot
    (let* ((tbl (find-table from))
           (schema (schema tbl))
           (rows (row-list tbl)))
      (when inner-join
        (multiple-value-setq (rows schema) (join-rows rows schema inner-join)))
      (when where
        (setf rows (restrict-rows rows (compile-where where schema))))
      (when order-by
        (setf rows (sorted-rows rows schema order-by)))
      (unless (eql col t)
        (setf schema (extract-schema (mklist col) schema))
        (setf rows (project-columns rows schema)))
      (when distinct
        (setf rows (distinct-rows rows schema)))
      (when (or limit offset)
        (setf rows (slice-rows rows limit offset)))
      (make-result-table rows schema))))

(defun make-result-table (rows schema)
  (make-instance 'table
                 :schema schema
                 :name (gensym "SELECT")
                 :pk (length rows)
                 :rows (fset:convert 'fset:map
                                     (loop for row in rows
                                           for id from 1
                                           collect (cons id row)))))

(defun mklist (thing)
  (if (listp thing) thing (list thing)))

(defun extract-schema (col-names schema)
  (iter (for c in col-names) (collect (find-column c schema))))

(defun restrict-rows (rows where)
  (remove-if-not where rows))

(defun project-columns (rows schema)
  (mapcar (extractor schema) rows))

(defun distinct-rows (rows schema)
  (remove-duplicates rows :test (row-equality-tester schema) :from-end t))

(defun slice-rows (rows limit offset)
  (dolist (n (list limit offset))
    (unless (typep n '(or null (integer 0)))
      (error 'query-error
             :message (format nil "LIMIT and OFFSET must be whole numbers, 0 or more, got ~S" n))))
  (let* ((start (min (or offset 0) (length rows)))
         (end (if limit (min (+ start limit) (length rows)) (length rows))))
    (subseq rows start end)))

(defun join-rows (rows schema inner-join)
  (destructuring-bind (other-name here there) inner-join
    (let* ((other (find-table other-name))
           (here-col (find-column here schema))
           (there-col (find-column there (schema other)))
           (other-rows (row-list other))
           (clash (intersection (mapcar #'name schema) (mapcar #'name (schema other)))))
      (when clash
        (error 'query-error
               :message (format nil "Can't join: both tables have columns named ~A" clash)))
      (values (loop for a in rows
                    nconc (loop for b in other-rows
                                when (let ((x (getf a (name here-col)))
                                           (y (getf b (name there-col))))
                                       (and (not (null-value-p x))
                                            (not (null-value-p y))
                                            (funcall (equality-predicate here-col) x y)))
                                  collect (append a b)))
              (append schema (schema other))))))

(defun extractor (schema)
  (let ((names (mapcar #'name schema)))
    #'(lambda (row) (iter (for c in names) (collect c) (collect (getf row c))))))

(defun row-equality-tester (schema)
  (let ((names (mapcar #'name schema))
        (tests (mapcar #'equality-predicate schema)))
    #'(lambda (a b)
        (iter (for name in names)
              (for test in tests)
              (always (let ((x (getf a name))
                            (y (getf b name)))
                        ;; two NULLs count as equal here, so DISTINCT keeps one
                        (cond ((and (null-value-p x) (null-value-p y)) t)
                              ((or (null-value-p x) (null-value-p y)) nil)
                              (t (funcall test x y)))))))))

;;; Sorting
;;;
;;; A key is a column, or (asc expr) or (desc expr), with the option
;;; :nulls :first or :nulls :last. Any other list is an expression key that
;;; sorts ascending. NULL sorts last unless the key says :nulls :first.
;;; ORDER-BY is one key or a list of keys. A list that starts with asc or desc is
;;; one key, so (desc :age) works without extra brackets. A column named ASC
;;; or DESC goes in a wrapper: ((asc :desc)).

(defun direction-form-p (x)
  (and (consp x)
       (symbolp (first x))
       (member (symbol-name (first x)) '("ASC" "DESC") :test #'string=)
       t))

;; Return (direction expression nulls).
(defun parse-order-key (item)
  (if (direction-form-p item)
      (destructuring-bind (direction expr &rest options) item
        (let ((nulls (getf options :nulls :last)))
          (unless (member nulls '(:first :last))
            (error 'query-error :message (format nil ":nulls must be :first or :last, got ~S" nulls)))
          (list (if (string= (symbol-name direction) "DESC") :desc :asc) expr nulls)))
      (list :asc item :last)))

(defun order-keys (order-by)
  (cond ((null order-by) nil)
        ((direction-form-p order-by) (list (parse-order-key order-by)))
        ((listp order-by) (mapcar #'parse-order-key order-by))
        (t (list (parse-order-key order-by)))))

;; Return (value-function less direction nulls). A column sorts by its own
;; comparator. Any other expression sorts with VALUE-LESS.
(defun compile-order-key (key schema)
  (destructuring-bind (direction expr nulls) key
    (let* ((column (and (keywordp expr) (not (eq expr :null)) (find-column expr schema)))
           (value-fn (compile-expr expr (make-ectx schema))))
      (list value-fn (or (and column (comparator column)) #'value-less) direction nulls))))

;; :lt, :eq or :gt for two lists of key values
(defun compare-key-values (a-values b-values keys)
  (loop for a in a-values
        for b in b-values
        for (nil less direction nulls) in keys
        do (let ((order (cond ((and (null-value-p a) (null-value-p b)) :eq)
                              ((null-value-p a) (if (eq nulls :first) :lt :gt))
                              ((null-value-p b) (if (eq nulls :first) :gt :lt))
                              ((funcall less a b) (if (eq direction :asc) :lt :gt))
                              ((funcall less b a) (if (eq direction :asc) :gt :lt))
                              (t :eq))))
             (unless (eq order :eq)
               (return order)))
        finally (return :eq)))

(defun sorted-rows (rows schema order-by)
  (if (functionp order-by)
      (stable-sort (copy-list rows) order-by)
      (let* ((keys (mapcar #'(lambda (key) (compile-order-key key schema)) (order-keys order-by)))
             (decorated (mapcar #'(lambda (row)
                                    (cons (mapcar #'(lambda (key) (funcall (first key) row)) keys)
                                          row))
                                rows)))
        (mapcar #'cdr
                (stable-sort decorated
                             #'(lambda (a b)
                                 (eq :lt (compare-key-values (car a) (car b) keys))))))))

(defun column-matcher (col val)
  (let ((name (name col))
        (predicate (equality-predicate col)))
    (if (null-value-p val)
        #'(lambda (row) (null-value-p (getf row name)))
        (let ((normalised (normalise-for-col val col)))
          #'(lambda (row)
              (let ((row-val (getf row name)))
                ;; a NULL column value never matches a given value
                (and (not (null-value-p row-val))
                     (funcall predicate row-val normalised))))))))

;; A nil value means "any value". The value :null matches rows where the column is NULL.
(defun column-matchers (schema names-and-values)
  (iter (for (name val) on names-and-values by #'cddr)
        (when val
          (collect (column-matcher (find-column name schema) val)))))

;; Build a WHERE function: (matching "table" :col value ...)
(defun matching (table &rest names-and-values)
  (let ((matchers (column-matchers (schema (find-table table)) names-and-values)))
    #'(lambda (row) (every #'(lambda (matcher) (funcall matcher row)) matchers))))
