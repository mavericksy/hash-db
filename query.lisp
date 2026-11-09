;;;; Queries: select and its helpers.

(in-package :hash-db)

;; Run a query.
;; WHERE is a function of one row (see MATCHING), the name of one, or an
;; expression (see expr.lisp).
;; INNER-JOIN, LEFT-JOIN, RIGHT-JOIN, FULL-JOIN and JOINS add other tables.
;; A join is (table-name column-here column-there). See the notes on joins below.
;; COL is t for all columns, one item, or a list of items. An item is a
;; column, or (as expression alias), or an expression. See SELECT-ITEMS.
;; ORDER-BY is a column, an expression key, a list of keys, or a function of
;; two rows that is true when the first row sorts first. See COMPILE-ORDER-KEY.
;; LIMIT and OFFSET cut the result after sorting.
;; Return a new table. Its rows are keyed 1..n in result order.
(defun select (&key (col t) from where distinct order-by inner-join limit offset
                 group-by having left-join right-join full-join joins)
  (with-snapshot
    (let* ((tbl (find-table from))
           (schema (schema tbl))
           (rows (row-list tbl)))
      (loop for (kind . spec) in (all-joins inner-join left-join right-join full-join joins)
            do (multiple-value-setq (rows schema) (join-rows rows schema kind spec)))
      (when where
        (setf rows (restrict-rows rows (compile-where where schema))))
      (multiple-value-setq (rows schema)
        (if (grouped-query-p col group-by having order-by)
            (select-groups rows schema col group-by having order-by)
            (select-rows rows schema col order-by)))
      (when distinct
        (setf rows (distinct-rows rows schema)))
      (when (or limit offset)
        (setf rows (slice-rows rows limit offset)))
      (make-result-table rows schema))))

;; Sort and project rows. Return the new rows and the schema of the result.
(defun select-rows (rows schema col order-by)
  (let ((items (unless (eql col t) (select-items col schema))))
    (when order-by
      (setf rows (sorted-rows rows (make-ectx schema (item-aliases items)) order-by)))
    (if items
        (values (project-rows rows items) (mapcar #'second items))
        (values rows schema))))

;;; Grouping
;;;
;;; GROUP-BY is a column, an expression, or a list of them. A list is always a list of
;;; expressions, so one expression needs brackets: :group-by '((lower :city)).
;;; A query is grouped when it has :group-by or :having, or when :col or :order-by holds
;;; an aggregate. Without :group-by, all rows make one group, even if there are none.
;;; A grouped query needs :col. :col, :having and :order-by can use the :group-by
;;; expressions as they are written, aliases, and aggregates: (count), (count x),
;;; (count-distinct x), (sum x), (avg x), (min x) and (max x). NULL values are skipped.
;;; Groups come out in the order their first row appeared.

(defun expression-list (x)
  (cond ((null x) nil)
        ((keywordp x) (list x))
        ((listp x) x)
        (t (list x))))

(defun grouped-query-p (col group-by having order-by)
  (or group-by
      having
      (and (not (eql col t))
           (some #'contains-aggregate-p (if (listp col) col nil)))
      (and order-by
           (not (functionp order-by))
           (some #'contains-aggregate-p
                 (if (and (listp order-by) (not (direction-form-p order-by)))
                     order-by
                     (list order-by))))))

(defun build-groups (rows key-fns)
  (let ((table (make-hash-table :test 'equal))
        (order '()))
    (dolist (row rows)
      (let* ((keys (mapcar #'(lambda (f) (funcall f row)) key-fns))
             (id (mapcar #'distinct-key keys))
             (group (gethash id table)))
        (unless group
          (setf group (make-grp nil keys)
                (gethash id table) group)
          (push group order))
        (push row (grp-rows group))))
    (dolist (group order)
      (setf (grp-rows group) (nreverse (grp-rows group))))
    (nreverse order)))

(defun select-groups (rows schema col group-by having order-by)
  (when (eql col t)
    (error 'query-error :message "A query with :group-by or aggregates needs :col"))
  (let* ((group-exprs (expression-list group-by))
         (key-fns (mapcar #'(lambda (expr) (compile-value expr (make-ectx schema))) group-exprs))
         (groups (build-groups rows key-fns))
         (items (select-items col schema (make-ectx schema nil group-exprs t)))
         (ctx (make-ectx schema (item-aliases items) group-exprs t)))
    (when (and (null group-exprs) (null groups))
      (setf groups (list (make-grp nil nil))))
    (when having
      (let ((keep (compile-expr having ctx)))
        (setf groups (remove-if-not #'(lambda (group) (eq t (truth (funcall keep group)))) groups))))
    (when (and order-by (not (functionp order-by)))
      (setf groups (sorted-rows groups ctx order-by)))
    (let ((result (project-rows groups items)))
      (when (functionp order-by)
        (setf result (stable-sort result order-by)))
      (values result (mapcar #'second items)))))

(defun make-result-table (rows schema)
  (make-instance 'table
                 :schema schema
                 :name (gensym "SELECT")
                 :pk (length rows)
                 :rows (fset:convert 'fset:map
                                     (loop for row in rows
                                           for id from 1
                                           collect (cons id row)))))

(defun restrict-rows (rows where)
  (remove-if-not where rows))

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

;;; Joins
;;;
;;; A join is (table here there :as prefix). HERE is a column of the rows so far,
;;; THERE is a column of the joined table. Rows join where the two values are equal.
;;; NULL never matches. :inner-join, :left-join, :right-join and :full-join each take
;;; one join or a list of joins. They apply in that order. For another order, use
;;; :joins, a list of (kind table here there :as prefix) with kind :inner, :left,
;;; :right or :full.
;;;
;;; Left keeps the rows with no match and gives them NULL for the new columns. Right
;;; keeps the unmatched rows of the joined table, with NULL for the earlier columns,
;;; after the matched rows. Full does both.
;;;
;;; Two tables can't have a column with the same name. Give the joined table an
;;; :as prefix, and its columns are named :PREFIX.COLUMN. THERE still uses the
;;; plain name.

(defun join-list (kind spec)
  "SPEC is nil, one join, or a list of joins. Return a list of (kind . join)."
  (cond ((null spec) nil)
        ((consp (first spec)) (mapcar #'(lambda (join) (cons kind join)) spec))
        (t (list (cons kind spec)))))

(defun all-joins (inner-join left-join right-join full-join joins)
  (append (join-list :inner inner-join)
          (join-list :left left-join)
          (join-list :right right-join)
          (join-list :full full-join)
          (mapcar #'(lambda (join) (cons (first join) (rest join))) joins)))

(defun null-row (schema)
  (loop for column in schema append (list (name column) :null)))

(defun prefixed-name (prefix name)
  (as-keyword (format nil "~A.~A" prefix name)))

(defun join-rows (rows schema kind spec)
  (unless (member kind '(:inner :left :right :full))
    (error 'query-error :message (format nil "Unknown join kind ~S" kind)))
  (unless (and (consp spec) (>= (length spec) 3) (evenp (length (cdddr spec))))
    (error 'query-error
           :message (format nil "A join is (table here there :as prefix), got ~S" spec)))
  (destructuring-bind (other-name here there &key as) spec
    (let* ((other (find-table other-name))
           (here-col (find-column here schema))
           (there-col (find-column there (schema other)))
           (other-schema (if as
                             (mapcar #'(lambda (column)
                                         (make-result-column (prefixed-name as (name column)) column))
                                     (schema other))
                             (schema other)))
           (there-name (name (find-column (if as (prefixed-name as (name there-col)) (name there-col))
                                          other-schema)))
           (other-rows (if as
                           (mapcar #'(lambda (row)
                                       (loop for (key value) on row by #'cddr
                                             append (list (prefixed-name as key) value)))
                                   (row-list other))
                           (row-list other)))
           (clash (intersection (mapcar #'name schema) (mapcar #'name other-schema))))
      (when clash
        (error 'query-error
               :message (format nil "Can't join: both tables have columns named ~A. Give ~A an :as prefix."
                                clash other-name)))
      (let* ((others (coerce other-rows 'vector))
             (matched (make-array (length others) :initial-element nil))
             (null-others (null-row other-schema))
             (null-here (null-row schema))
             (equal-p (equality-predicate here-col))
             (result '()))
        (dolist (row rows)
          (let ((x (getf row (name here-col)))
                (hit nil))
            (unless (null-value-p x)
              (loop for other-row across others
                    for i from 0
                    do (let ((y (getf other-row there-name)))
                         (when (and (not (null-value-p y)) (funcall equal-p x y))
                           (setf hit t
                                 (aref matched i) t)
                           (push (append row other-row) result)))))
            (when (and (not hit) (member kind '(:left :full)))
              (push (append row null-others) result))))
        (when (member kind '(:right :full))
          (loop for other-row across others
                for i from 0
                unless (aref matched i)
                  do (push (append null-here other-row) result)))
        (values (nreverse result) (append schema other-schema))))))

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

;;; Select items
;;;
;;; COL is t (every column), one item, or a list of items. An item is
;;;   :age                    a column
;;;   (as expression :alias)  an expression with a name. The expression can be
;;;                           a column, which renames it.
;;;   (+ :age 1)              an expression. It is named :EXPR1, :EXPR2 ...
;;;                           by its place in the list.
;;; A list in COL is always a list of items, so one expression needs brackets
;;; around it: :col '((+ :age 1)).

(defun as-form-p (x)
  (and (consp x) (symbolp (first x)) (string= (symbol-name (first x)) "AS")))

;; The name of an item that has no (as ...): :COUNT for (count), :SUM-AGE for
;; (sum :age), else :EXPRn where n is the place in the list.
(defun default-alias (expr position)
  (let ((aggregate (and (consp expr)
                        (symbolp (first expr))
                        (member (symbol-name (first expr)) *aggregate-names* :test #'string=)
                        (symbol-name (first expr)))))
    (as-keyword
     (cond ((and aggregate (null (rest expr))) aggregate)
           ((and aggregate (null (cddr expr)) (keywordp (second expr)))
            (format nil "~A-~A" aggregate (symbol-name (second expr))))
           (t (format nil "EXPR~D" position))))))

;; A column made for a result: it has a name, a comparison and an equality.
(defun make-result-column (name &optional source)
  (make-instance 'column
                 :name name
                 :value-type (if source (value-type source) t)
                 :comparator (if source (or (comparator source) #'value-less) #'value-less)
                 :equality-predicate (if source (equality-predicate source) #'value-equal)))

;; Return a list of (name column function). The function takes a row.
(defun select-items (col schema &optional (ctx (make-ectx schema)))
  (let ((specs (cond ((keywordp col) (list col))
                     ((and col (listp col)) col)
                     (t (error 'query-error :message (format nil "Bad :col ~S" col)))))
        (items '()))
    (loop for spec in specs
          for position from 1
          do (push
              (cond ((and (keywordp spec) (not (eq spec :null)))
                     (let ((column (find-column spec schema)))
                       (list (name column) column (compile-expr spec ctx))))
                    (t
                     (multiple-value-bind (expr alias)
                         (if (as-form-p spec)
                             (progn (unless (= 3 (length spec))
                                      (error 'query-error
                                             :message (format nil "AS takes an expression and a name: ~S" spec)))
                                    (values (second spec) (as-keyword (third spec))))
                             (values spec (default-alias spec position)))
                       (let ((source (and (keywordp expr) (not (eq expr :null))
                                          (find-column expr schema))))
                         (list alias (make-result-column alias source)
                               (compile-value expr ctx))))))
              items))
    (setf items (nreverse items))
    (let ((names (mapcar #'first items)))
      (when (/= (length names) (length (remove-duplicates names)))
        (error 'query-error :message (format nil "Two result columns have the same name: ~S" names))))
    items))

;; Aliases are the names of items that can be used in ORDER-BY.
;; Each is (name function column).
(defun item-aliases (items)
  (mapcar #'(lambda (item) (list (first item) (third item) (second item))) items))

(defun project-rows (rows items)
  (mapcar #'(lambda (row)
              (loop for (name nil fn) in items
                    append (list name (funcall fn row))))
          rows))

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

;; Return (value-function less direction nulls) for KEY. CTX says what names an
;; expression can use. A column sorts by its own
;; comparator. Any other expression sorts with VALUE-LESS.
(defun compile-order-key (key ctx)
  (destructuring-bind (direction expr nulls) key
    (let* ((alias (and (keywordp expr) (find expr (ectx-aliases ctx) :key #'first)))
           (column (cond (alias (third alias))
                         ((and (keywordp expr) (not (eq expr :null)))
                          (find-column expr (ectx-schema ctx)))))
           (value-fn (compile-value expr ctx)))
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

(defun sorted-rows (rows ctx order-by)
  (if (functionp order-by)
      (stable-sort (copy-list rows) order-by)
      (let* ((keys (mapcar #'(lambda (key) (compile-order-key key ctx))
                           (order-keys order-by)))
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

;;; Changing many rows
;;;
;;; update-where and delete-where run in one transaction, so all the rows change or
;;; none do. They need :where, as a function, a function name or an expression.
;;; Use :where t to change every row. They return the number of rows changed.

(defun require-where (where who)
  (when (null where)
    (error 'query-error
           :message (format nil "~A needs :where. Use :where t to change every row." who))))

;; The ids of the rows of TBL that pass PREDICATE, in id order.
(defun matching-row-ids (tbl predicate)
  (let ((ids '()))
    (fset:do-map (id row (rows tbl))
      (when (funcall predicate row)
        (push id ids)))
    (nreverse ids)))

;; Change the rows that pass WHERE.
;; CHANGES is a list of :column value pairs, or a function of the old row that
;; returns such a list.
;; COMPUTE is a list of :column expression pairs. Each expression is worked out on the
;; old row, so (:age (+ :age 1)) adds one to age.
(defun update-where (table changes &key where compute)
  (require-where where "update-where")
  (with-transaction
    (let* ((tbl (stored-table table))
           (schema (schema tbl))
           (predicate (compile-where where schema))
           (ctx (make-ectx schema))
           (computed (loop for (column expr) on compute by #'cddr
                           collect (list (name (find-column column schema))
                                         (compile-value expr ctx)))))
      (when (and (listp changes)
                 (intersection (mapcar #'first computed)
                               (loop for key in changes by #'cddr collect (as-keyword key))))
        (error 'query-error :message "A column is in both the changes and :compute"))
      (let ((ids (matching-row-ids tbl predicate)))
        (dolist (id ids)
          (let ((row (fset:lookup (rows tbl) id)))
            (update-row id
                        (append (if (functionp changes) (funcall changes row) changes)
                                (loop for (name fn) in computed
                                      append (list name (funcall fn row))))
                        table)))
        (length ids)))))

;; Delete the rows that pass WHERE. Cascades and restrictions apply to each row.
;; The count is the rows deleted, not the rows deleted by cascades.
(defun delete-where (table &key where)
  (require-where where "delete-where")
  (with-transaction
    (let* ((tbl (stored-table table))
           (ids (matching-row-ids tbl (compile-where where (schema tbl)))))
      (count-if #'identity
                (mapcar #'(lambda (id) (delete-row id table)) ids)))))
