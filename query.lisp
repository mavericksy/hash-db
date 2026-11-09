;;;; Queries: select and its helpers.

(in-package :hash-db)

;; Run a query. WHERE is a function of one row, see MATCHING.
;; INNER-JOIN is (table-name column-here column-there).
;; Return a new table. Its rows are keyed 1..n in result order.
(defun select (&key (col t) from where distinct order-by inner-join)
  (with-snapshot
    (let* ((tbl (find-table from))
           (schema (schema tbl))
           (rows (row-list tbl)))
      (when inner-join
        (multiple-value-setq (rows schema) (join-rows rows schema inner-join)))
      (when where
        (setf rows (restrict-rows rows where)))
      (when order-by
        (setf rows (sorted-rows rows schema (mklist order-by))))
      (unless (eql col t)
        (setf schema (extract-schema (mklist col) schema))
        (setf rows (project-columns rows schema)))
      (when distinct
        (setf rows (distinct-rows rows schema)))
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

(defun sorted-rows (rows schema order-by)
  (stable-sort (copy-list rows) (row-comparator order-by schema)))

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

;; True when row A sorts before row B. The first column that differs decides.
;; NULL sorts after every other value.
(defun row-comparator (col-names schema)
  (let* ((names (mapcar #'as-keyword col-names))
         (comparators (mapcar #'(lambda (col)
                                  (or (comparator col)
                                      (error 'query-error
                                             :message (format nil "Column ~A has no comparator" (name col)))))
                              (extract-schema names schema))))
    #'(lambda (a b)
        (loop for name in names
              for comp in comparators
              do (let ((a-v (getf a name))
                       (b-v (getf b name)))
                   (cond ((and (null-value-p a-v) (null-value-p b-v)))
                         ((null-value-p a-v) (return nil))
                         ((null-value-p b-v) (return t))
                         ((funcall comp a-v b-v) (return t))
                         ((funcall comp b-v a-v) (return nil))))
              finally (return nil)))))

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
