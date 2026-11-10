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
           (join-list (all-joins inner-join left-join right-join full-join joins))
           (rows (base-rows tbl where join-list)))
      (loop for (kind . spec) in join-list
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

;;; Access plans
;;;
;;; A :where that is an expression can often be answered from an index. The planner
;;; looks at the parts of an AND that compare one column of the table with
;;; constants: = < > <= >= between and in. It picks the one that should find the
;;; fewest rows and reads the matching ids from the index. The whole :where still
;;; runs on those rows, so the plan can only make a query faster. The rows come out
;;; in id order, as a scan gives them.
;;;
;;; A function in :where, an OR, a NOT, and any other shape make a scan. So does a
;;; column without an index.

(defun op-name-of (expr)
  (and (consp expr) (symbolp (first expr)) (symbol-name (first expr))))

;; True when EXPR names no column and calls no function of yours, so it has one
;; value for the whole query.
(defun constant-expr-p (expr)
  (cond ((eq expr :null) t)
        ((keywordp expr) nil)
        ((or (null expr) (eq expr t)) t)
        ((symbolp expr) nil)
        ((atom expr) t)
        (t (let ((name (op-name-of expr)))
             (and name
                  (not (string= name "FN"))
                  (not (member name *aggregate-names* :test #'string=))
                  (or (string= name "QUOTE")
                      (every #'constant-expr-p (rest expr))))))))

;; Return the value of a constant expression and t. If it can't be worked out, nil.
(defun constant-value (expr)
  (handler-case (values (materialize (funcall (compile-expr expr (make-ectx nil)) nil)) t)
    (hash-db-error () (values nil nil))))

(defun conjuncts (where)
  (if (equal (op-name-of where) "AND")
      (loop for part in (rest where) append (conjuncts part))
      (list where)))

(defun flip-comparison (name)
  (cdr (assoc name '(("<" . ">") (">" . "<") ("<=" . ">=") (">=" . "<=") ("=" . "="))
              :test #'string=)))

;; Describe a comparison of one column with constants as (column kind . details), or
;; return nil. Kinds: (:eq value), (:in values), (:range low low-inclusive high high-inclusive).
(defun sargable (conjunct schema)
  (let ((name (op-name-of conjunct))
        (args (rest conjunct)))
    (flet ((column-of (x)
             (and (keywordp x) (not (eq x :null)) (find x schema :key #'name)))
           (const (x)
             (and (constant-expr-p x) (multiple-value-list (constant-value x)))))
      (cond
        ((and name (member name '("=" "<" ">" "<=" ">=") :test #'string=) (= 2 (length args)))
         (destructuring-bind (a b) args
           (let* ((flipped (column-of b))
                  (column (or (column-of a) flipped))
                  (op (if (column-of a) name (and flipped (flip-comparison name))))
                  (value (and column (const (if (column-of a) b a)))))
             (when (and column value (second value) op)
               (let ((v (first value)))
                 (list column
                       (cond ((string= op "=") (list :eq v))
                             ((string= op "<") (list :range nil nil v nil))
                             ((string= op "<=") (list :range nil nil v t))
                             ((string= op ">") (list :range v nil nil nil))
                             (t (list :range v t nil nil)))))))))
        ((and (equal name "BETWEEN") (= 3 (length args)) (column-of (first args)))
         (let ((lo (const (second args))) (hi (const (third args))))
           (when (and lo hi (second lo) (second hi))
             (list (column-of (first args)) (list :range (first lo) t (first hi) t)))))
        ((and (equal name "IN") (>= (length args) 2) (column-of (first args))
              (every #'constant-expr-p (rest args)))
         (let ((values (mapcar #'constant-value (rest args))))
           (when (every #'(lambda (arg) (second (multiple-value-list (constant-value arg)))) (rest args))
             (list (column-of (first args)) (list :in values)))))))))

;; Cut two range limits down to the tighter one. A bound is (value inclusive).
(defun tighter-bound (a b side)
  (cond ((null (first a)) b)
        ((null (first b)) a)
        (t (let ((order (value-compare (first a) (first b))))
             (cond ((eq order :eq) (list (first a) (and (second a) (second b))))
                   ((eq side :low) (if (eq order :gt) a b))
                   (t (if (eq order :lt) a b)))))))

;; Join the :range parts on one column, so (and (> :a 1) (< :a 9)) is one range.
(defun merge-sargs (sargs)
  (let ((merged '()))
    (dolist (sarg sargs (nreverse merged))
      (destructuring-bind (column (kind . details) &aux (same (and (eq kind :range)
                                                                  (find-if #'(lambda (m) (and (eq (first m) column)
                                                                                              (eq (first (second m)) :range)))
                                                                           merged))))
          sarg
        (if same
            (destructuring-bind (low low-inclusive high high-inclusive) (rest (second same))
              (let ((new-low (tighter-bound (list low low-inclusive) (list (first details) (second details)) :low))
                    (new-high (tighter-bound (list high high-inclusive) (list (third details) (fourth details)) :high)))
                (setf (second same) (list :range (first new-low) (second new-low)
                                          (first new-high) (second new-high)))))
            (push (list column (cons kind details)) merged))))))

;; Lower is better.
(defun sarg-score (tbl sarg)
  (destructuring-bind (column (kind . details)) sarg
    (let ((primary (typep column 'primary-key-column))
          (index (table-index-for tbl (name column))))
      (cond ((and primary (member kind '(:eq :in))) (if (eq kind :eq) 0 3))
            (primary nil)
            ((null index) nil)
            ((eq kind :eq) (if (index-unique index) 1 2))
            ((eq kind :in) 3)
            ((not (range-column-p column)) nil)
            ((and (first details) (third details)) 4)
            (t 5)))))

(defun key-kind-matches-p (column value)
  (if (subtypep (value-type column) 'real) (realp value) (stringp value)))

;; The ids a comparison selects, in any order, or :unusable when the index can't answer.
(defun sarg-ids (tbl sarg)
  (destructuring-bind (column (kind . details)) sarg
    (let ((index (table-index-for tbl (name column)))
          (primary (typep column 'primary-key-column)))
      (flet ((lookup (value)
               (cond ((null-value-p value) nil)
                     (primary (let ((id (and (realp value) (integerp (rational value)) (rational value))))
                                (and id (nth-value 1 (fset:lookup (rows tbl) id)) (list id))))
                     (t (index-ids index value)))))
        (ecase kind
          (:eq (lookup (first details)))
          (:in (loop for value in (first details) append (lookup value)))
          (:range
           (destructuring-bind (low low-inclusive high high-inclusive) details
             (cond ((or (and low (null-value-p low)) (and high (null-value-p high))) nil)
                   ((and (or (null low) (key-kind-matches-p column low))
                         (or (null high) (key-kind-matches-p column high)))
                    (index-range-ids index low low-inclusive high high-inclusive))
                   (t :unusable)))))))))

;; Choose how to read the rows of TBL for WHERE. Return a list:
;;   (:kind :scan)
;;   (:kind :index :index name :column c :op :eq/:in/:range :ids ids)
;; An :index plan reads the primary key too, with :index :primary-key.
(defun plan-access (tbl where)
  (let ((scan (list :kind :scan)))
    (if (or (null where) (functionp where) (not (consp where)))
        scan
        (let ((candidates (loop for sarg in (merge-sargs (loop for part in (conjuncts where)
                                                                when (consp part)
                                                                  append (let ((s (sargable part (schema tbl))))
                                                                           (and s (list s)))))
                                for score = (sarg-score tbl sarg)
                                when score collect (cons score sarg))))
          (loop for (nil . sarg) in (stable-sort candidates #'< :key #'car)
                for ids = (sarg-ids tbl sarg)
                unless (eq ids :unusable)
                  do (return (list :kind :index
                                   :index (if (typep (first sarg) 'primary-key-column)
                                              :primary-key
                                              (index-name (table-index-for tbl (name (first sarg)))))
                                   :column (name (first sarg))
                                   :op (first (second sarg))
                                   :ids (sort (remove-duplicates ids) #'<)))
                finally (return scan))))))

(defun all-row-ids (tbl)
  (let ((ids '()))
    (fset:do-map (id row (rows tbl))
      (declare (ignorable row))
      (push id ids))
    (nreverse ids)))

;; The ids a plan reads, in id order.
(defun plan-ids (tbl plan)
  (if (eq (getf plan :kind) :scan)
      (all-row-ids tbl)
      (getf plan :ids)))

;; Right and full joins keep rows of the first table that have no match, so the
;; first table must not be cut down before the join.
(defun plan-allowed-p (joins)
  (notany #'(lambda (join) (member (car join) '(:right :full))) joins))

(defun base-rows (tbl where joins)
  (if (and where (plan-allowed-p joins))
      (let ((plan (plan-access tbl where)))
        (if (eq (getf plan :kind) :scan)
            (row-list tbl)
            (mapcar #'(lambda (id) (fset:lookup (rows tbl) id)) (getf plan :ids))))
      (row-list tbl)))

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
           (rename (if as
                       #'(lambda (row)
                           (loop for (key value) on row by #'cddr
                                 append (list (prefixed-name as key) value)))
                       #'identity))
           (clash (intersection (mapcar #'name schema) (mapcar #'name other-schema))))
      (when clash
        (error 'query-error
               :message (format nil "Can't join: both tables have columns named ~A. Give ~A an :as prefix."
                                clash other-name)))
      (join-matches rows schema kind (name here-col) other other-schema there-name there-col rename))))

;;; How rows find their partners. Rows of the two tables join where the two values
;;; are equal, and NULL never matches. Two ways to find them, both without trying
;;; every pair:
;;;   - an index on the joined column, when the join is inner or left and fewer
;;;     than a quarter as many rows look as the joined table holds. Each row looks
;;;     up its partners in the index, and nothing is built.
;;;   - a hash table over the joined table, built once for the query. Right and full
;;;     joins need it, because they also report the rows that nobody matched.
;;; Rows come out in the order of the first table, and the partners of one row in the
;;; order of the joined table, as a nested loop would give them. A joined row that no
;;; row matched comes last.

(defun join-matches (rows schema kind here-name other other-schema there-name there-col rename)
  (let* ((index (and (member kind '(:inner :left))
                     ;; Building the hash table costs one pass over the joined table.
                     ;; Looking up in the index costs more per row, so it pays only
                     ;; when few rows look.
                     (< (* 4 (length rows)) (fset:size (rows other)))
                     (table-index-for other (name there-col))))
         (others (unless index
                   (coerce (mapcar rename (row-list other)) 'vector)))
         (positions (unless index
                      (let ((table (make-hash-table :test 'equal)))
                        (loop for other-row across others
                              for i from 0
                              for y = (getf other-row there-name)
                              unless (null-value-p y)
                                do (push i (gethash (distinct-key y) table)))
                        table)))
         (matched (unless index (make-array (length others) :initial-element nil)))
         (null-others (null-row other-schema))
         (null-here (null-row schema))
         (result '()))
    (dolist (row rows)
      (let* ((x (getf row here-name))
             (hits (cond ((null-value-p x) nil)
                         (index
                          (mapcar #'(lambda (id) (funcall rename (fset:lookup (rows other) id)))
                                  (index-ids index x)))
                         (t
                          (let ((found (reverse (gethash (distinct-key x) positions))))
                            (dolist (i found)
                              (setf (aref matched i) t))
                            (mapcar #'(lambda (i) (aref others i)) found))))))
        (dolist (other-row hits)
          (push (append row other-row) result))
        (when (and (null hits) (member kind '(:left :full)))
          (push (append row null-others) result))))
    (when (member kind '(:right :full))
      (loop for other-row across others
            for i from 0
            unless (aref matched i)
              do (push (append null-here other-row) result)))
    (values (nreverse result) (append schema other-schema))))

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
(defun matching-row-ids (tbl where predicate)
  (loop for id in (plan-ids tbl (plan-access tbl where))
        when (funcall predicate (fset:lookup (rows tbl) id))
          collect id))

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
      (let ((ids (matching-row-ids tbl where predicate)))
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
           (ids (matching-row-ids tbl where (compile-where where (schema tbl)))))
      (count-if #'identity
                (mapcar #'(lambda (id) (delete-row id table)) ids)))))

;;; Explain
;;;
;;; EXPLAIN takes the same arguments as SELECT and returns the steps SELECT would take,
;;; without running the query. The access step reads the index it would use, so its row
;;; count is exact. Later steps do not run, so they have no counts.
;;;
;;; Each step is a list that starts with :step and a kind:
;;;   (:step :scan :table t :access :full-scan :rows n :reason r)
;;;   (:step :scan :table t :access :index :index i :column c :op o :rows n)
;;;   (:step :join :kind k :table t :on (here there) :method m :index i)
;;;   (:step :filter :where w)
;;;   (:step :group :by exprs :having h)
;;;   (:step :sort :keys keys)         each key is (:key e :direction d :nulls n)
;;;   (:step :project :columns names)
;;;   (:step :distinct)
;;;   (:step :limit :limit n :offset m)
;;; The access step says :reason when a :where did not use an index: :function,
;;; :right-or-full-join or :no-index-condition. A join method is :index, :hash, or
;;; :index-or-hash when it depends on how many rows reach the join. Only the first
;;; join knows that.

(defun item-names (col)
  (loop for spec in (cond ((eq col t) nil)
                          ((keywordp col) (list col))
                          (t col))
        for position from 1
        collect (cond ((and (keywordp spec) (not (eq spec :null))) spec)
                      ((as-form-p spec) (as-keyword (third spec)))
                      (t (default-alias spec position)))))

(defun explain (&key (col t) from where distinct order-by inner-join limit offset
                  group-by having left-join right-join full-join joins)
  (with-snapshot
    (let* ((tbl (find-table from))
           (join-list (all-joins inner-join left-join right-join full-join joins))
           (plan (and where (plan-allowed-p join-list) (plan-access tbl where)))
           (steps '()))
      (flet ((add-step (&rest step) (push (cons :step step) steps)))
        ;; the first table
        (if (and plan (eq (getf plan :kind) :index))
            (add-step :scan :table (name tbl) :access :index :index (getf plan :index)
                        :column (getf plan :column) :op (getf plan :op)
                        :rows (length (getf plan :ids)))
            (apply #'add-step :scan :table (name tbl) :access :full-scan :rows (fset:size (rows tbl))
                          (cond ((null where) nil)
                                ((not (plan-allowed-p join-list)) (list :reason :right-or-full-join))
                                ((not (consp where)) (list :reason :function))
                                (t (list :reason :no-index-condition)))))
        ;; the joins
        (loop for (kind . spec) in join-list
              for first = t then nil
              do (let* ((other (find-table (first spec)))
                        (index (and (member kind '(:inner :left))
                                    (table-index-for other (as-keyword (third spec)))))
                        (rows-in (if plan
                                     (length (plan-ids tbl plan))
                                     (fset:size (rows tbl)))))
                   (add-step :join :kind kind :table (name other) :on (list (second spec) (third spec))
                               :method (cond ((null index) :hash)
                                             ((not first) :index-or-hash)
                                             ((< (* 4 rows-in) (fset:size (rows other))) :index)
                                             (t :hash))
                               :index (and index (index-name index)))))
        (when where
          (add-step :filter :where (if (consp where) where :function)))
        (when (grouped-query-p col group-by having order-by)
          (add-step :group :by (expression-list group-by) :having having))
        (when order-by
          (add-step :sort :keys (if (functionp order-by)
                                :function
                                (mapcar #'(lambda (key)
                                            (destructuring-bind (direction expr nulls) key
                                              (list :key expr :direction direction :nulls nulls)))
                                        (order-keys order-by)))))
        (unless (eql col t)
          (add-step :project :columns (item-names col)))
        (when distinct
          (add-step :distinct))
        (when (or limit offset)
          (add-step :limit :limit limit :offset offset)))
      (nreverse steps))))
