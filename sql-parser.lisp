;;;; SQL: the parser.
;;;;
;;;; The parser turns tokens into a tree of plain lists. It does not look at tables, so it
;;;; does not know which names are columns. sql-translate.lisp does that.
;;;;
;;;; Expressions:
;;;;   (:lit value)               a number, a string, t, nil or :null
;;;;   (:param n)                 the nth ?, counting from 0
;;;;   (:col qualifier name)      a column. The qualifier is a string or nil.
;;;;   (:op name arg ...)         OR AND NOT = <> < > <= >= + - * / % || NEG
;;;;   (:is-null x negated)  (:in x list negated)  (:between x low high negated)
;;;;   (:like x pattern negated)
;;;;   (:call name args distinct star)

(in-package :hash-db)

(defvar *sql-text* "")
(defvar *tokens* #())
(defvar *pos* 0)
(defvar *param-count* 0)

(defmacro with-sql-parser ((text) &body body)
  `(let* ((*sql-text* ,text)
          (*tokens* (sql-tokenize *sql-text*))
          (*pos* 0)
          (*param-count* 0))
     ,@body))

(defun tok (&optional (n 0))
  (aref *tokens* (min (+ *pos* n) (1- (length *tokens*)))))

(defun advance ()
  (prog1 (tok)
    (when (< *pos* (1- (length *tokens*)))
      (incf *pos*))))

(defun parse-fail (format-control &rest args)
  (apply #'sql-fail *sql-text* (token-position (tok)) format-control args))

(defun word-token-p (token word)
  (and (eq (token-type token) :word)
       (not (token-quoted token))
       (string-equal (token-value token) word)))

(defun at-word (word &optional (n 0))
  (word-token-p (tok n) word))

(defun accept-word (word)
  (when (at-word word)
    (advance)
    t))

(defun expect-word (word)
  (or (accept-word word)
      (parse-fail "Expected ~A" (string-upcase word))))

(defun at-symbol (symbol &optional (n 0))
  (let ((token (tok n)))
    (and (eq (token-type token) :symbol) (string= (token-value token) symbol))))

(defun accept-symbol (symbol)
  (when (at-symbol symbol)
    (advance)
    t))

(defun expect-symbol (symbol)
  (or (accept-symbol symbol)
      (parse-fail "Expected ~A" symbol)))

(defun at-end-p ()
  (eq (token-type (tok)) :eof))

;; Words that can't be a table, column or alias name unless "quoted".
(defparameter *sql-reserved-words*
  '("SELECT" "FROM" "WHERE" "GROUP" "BY" "HAVING" "ORDER" "LIMIT" "OFFSET" "JOIN" "INNER" "LEFT"
    "RIGHT" "FULL" "OUTER" "CROSS" "NATURAL" "USING" "ON" "AND" "OR" "NOT" "IN" "IS" "NULL" "LIKE"
    "BETWEEN" "AS" "DISTINCT" "UNION" "INTERSECT" "EXCEPT" "INSERT" "INTO" "VALUES" "UPDATE"
    "SET" "DELETE" "CREATE" "DROP" "ALTER" "TABLE" "INDEX" "VIEW" "UNIQUE" "PRIMARY" "KEY"
    "FOREIGN" "REFERENCES" "DEFAULT" "CHECK" "CASE" "WHEN" "THEN" "ELSE" "END" "ASC" "DESC"
    "TRUE" "FALSE" "CONSTRAINT" "ADD" "COLUMN" "RENAME" "TO" "EXPLAIN" "ESCAPE" "SEQUENCE"))

(defun reserved-token-p (token)
  (and (eq (token-type token) :word)
       (not (token-quoted token))
       (member (token-value token) *sql-reserved-words* :test #'string-equal)))

;; A table, column or alias name. Return the text and whether it was quoted.
(defun parse-name (what)
  (let ((token (tok)))
    (if (and (eq (token-type token) :word)
             (or (token-quoted token) (not (reserved-token-p token))))
        (progn (advance)
               (values (token-value token) (token-quoted token)))
        (parse-fail "Expected ~A" what))))

;;; Expressions

(defun parse-expression ()
  (parse-or))

(defun parse-or ()
  (let ((left (parse-and)))
    (loop while (accept-word "OR")
          do (setf left (list :op "OR" left (parse-and))))
    left))

(defun parse-and ()
  (let ((left (parse-not)))
    (loop while (accept-word "AND")
          do (setf left (list :op "AND" left (parse-not))))
    left))

(defun parse-not ()
  (if (accept-word "NOT")
      (list :op "NOT" (parse-not))
      (parse-comparison)))

(defun parse-comparison ()
  (let ((left (parse-additive)))
    (cond
      ((some #'at-symbol '("=" "<>" "!=" "<" ">" "<=" ">="))
       (let ((op (token-value (advance))))
         (list :op (if (string= op "!=") "<>" op) left (parse-additive))))
      ((at-word "IS")
       (advance)
       (let ((negated (accept-word "NOT")))
         (expect-word "NULL")
         (list :is-null left negated)))
      (t
       (let ((negated (and (at-word "NOT")
                           (or (at-word "IN" 1) (at-word "BETWEEN" 1) (at-word "LIKE" 1))
                           (advance)
                           t)))
         (cond
           ((accept-word "IN")
            (expect-symbol "(")
            (if (at-word "SELECT")
                (prog1 (list :in-subquery left (parse-query) negated) (expect-symbol ")"))
                (let ((items (list (parse-expression))))
                  (loop while (accept-symbol ",") do (push (parse-expression) items))
                  (expect-symbol ")")
                  (list :in left (nreverse items) negated))))
           ((accept-word "BETWEEN")
            (let ((low (parse-additive)))
              (expect-word "AND")
              (list :between left low (parse-additive) negated)))
           ((accept-word "LIKE")
            (let ((pattern (parse-additive)))
              (when (at-word "ESCAPE")
                (parse-fail "ESCAPE is not supported. Use a backslash in the pattern"))
              (list :like left pattern negated)))
           (t left)))))))

(defun parse-additive ()
  (let ((left (parse-multiplicative)))
    (loop while (or (at-symbol "+") (at-symbol "-") (at-symbol "||"))
          do (let ((op (token-value (advance))))
               (setf left (list :op op left (parse-multiplicative)))))
    left))

(defun parse-multiplicative ()
  (let ((left (parse-unary)))
    (loop while (or (at-symbol "*") (at-symbol "/") (at-symbol "%"))
          do (let ((op (token-value (advance))))
               (setf left (list :op op left (parse-unary)))))
    left))

(defun parse-unary ()
  (cond ((accept-symbol "-")
         (let ((operand (parse-unary)))
           (if (and (eq (first operand) :lit) (realp (second operand)))
               (list :lit (- (second operand)))
               (list :op "NEG" operand))))
        ((accept-symbol "+") (parse-unary))
        (t (parse-primary))))

(defun parse-call-arguments (name)
  "After NAME and its open bracket. Return (args distinct star)."
  (cond ((accept-symbol ")") (list nil nil nil))
        ((accept-symbol "*")
         (expect-symbol ")")
         (list nil nil t))
        (t (let ((distinct (accept-word "DISTINCT")))
             (when (and distinct (not (string-equal name "COUNT")))
               (parse-fail "DISTINCT works only inside COUNT"))
             (let ((args (list (parse-expression))))
               (loop while (accept-symbol ",") do (push (parse-expression) args))
               (expect-symbol ")")
               (list (nreverse args) distinct nil))))))

(defun parse-primary ()
  (let ((token (tok)))
    (case (token-type token)
      (:number (advance) (list :lit (token-value token)))
      (:string (advance) (list :lit (token-value token)))
      (:param (advance) (list :param (prog1 *param-count* (incf *param-count*))))
      (:symbol
       (cond ((accept-symbol "(")
              (if (at-word "SELECT")
                  (prog1 (list :subquery (parse-query)) (expect-symbol ")"))
                  (prog1 (parse-expression) (expect-symbol ")"))))
             (t (parse-fail "Expected an expression"))))
      (:word
       (cond ((token-quoted token) (parse-column-reference))
             ((at-word "NULL") (advance) (list :lit :null))
             ((at-word "TRUE") (advance) (list :lit t))
             ((at-word "FALSE") (advance) (list :lit nil))
             ((at-word "CASE") (parse-case))
             ((and (at-word "EXISTS") (at-symbol "(" 1))
              (advance)
              (advance)
              (unless (at-word "SELECT")
                (parse-fail "Expected SELECT after EXISTS ("))
              (prog1 (list :exists (parse-query)) (expect-symbol ")")))
             ((at-word "CURRENT_TIMESTAMP") (advance) (list :call "NOW" nil nil nil))
             ((at-word "CURRENT_DATE") (advance) (list :call "TODAY" nil nil nil))
             ;; DATE '2024-01-31' and TIMESTAMP '2024-01-31 12:00:00'
             ((and (or (at-word "DATE") (at-word "TIMESTAMP")) (eq (token-type (tok 1)) :string))
              (let ((name (string-upcase (token-value (advance)))))
                (list :call name (list (list :lit (token-value (advance)))) nil nil)))
             ((and (at-word "EXTRACT") (at-symbol "(" 1))
              (advance)
              (advance)
              (let ((field (string-upcase (parse-name "a date field"))))
                (unless (member field '("YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND" "DOW") :test #'string=)
                  (parse-fail "EXTRACT can take YEAR, MONTH, DAY, HOUR, MINUTE, SECOND or DOW"))
                (expect-word "FROM")
                (let ((tree (parse-expression)))
                  (expect-symbol ")")
                  (list :call field (list tree) nil nil))))
             ((reserved-token-p token) (parse-fail "Expected an expression"))
             ((at-symbol "(" 1)
              (let ((name (string-upcase (token-value (advance)))))
                (advance)
                (destructuring-bind (args distinct star) (parse-call-arguments name)
                  (list :call name args distinct star))))
             (t (parse-column-reference))))
      (t (parse-fail "Expected an expression")))))

(defun parse-column-reference ()
  (let ((first (parse-name "a column name")))
    (if (accept-symbol ".")
        (list :col first (parse-name "a column name"))
        (list :col nil first))))

;; CASE [operand] WHEN a THEN b ... [ELSE c] END gives (:case operand ((a b) ...) else).
(defun parse-case ()
  (expect-word "CASE")
  (let ((operand (unless (or (at-word "WHEN") (at-word "END")) (parse-expression)))
        (whens '())
        (else nil))
    (unless (at-word "WHEN")
      (parse-fail "Expected WHEN"))
    (loop while (accept-word "WHEN")
          do (let ((condition (parse-expression)))
               (expect-word "THEN")
               (push (list condition (parse-expression)) whens)))
    (when (accept-word "ELSE")
      (setf else (parse-expression)))
    (expect-word "END")
    (list :case operand (nreverse whens) else)))

;; Parse TEXT as one expression and nothing else.
(defun sql-parse-expression (text)
  (with-sql-parser (text)
    (let ((ast (parse-expression)))
      (accept-symbol ";")
      (unless (at-end-p)
        (parse-fail "Unexpected text after the expression"))
      (values ast *param-count*))))

;;; Statements
;;;
;;; SELECT gives (:select :distinct d :items items :from (table quoted alias) :joins joins
;;;               :where e :group (e ...) :having e :order (key ...) :limit e :offset e)
;;; An item is (:star qualifier) or (:expr tree alias). An order key is (tree direction nulls).

(defun parse-select-item ()
  (cond ((accept-symbol "*") (list :star nil))
        ((and (eq (token-type (tok)) :word) (at-symbol "." 1) (at-symbol "*" 2))
         (let ((qualifier (parse-name "a table name")))
           (advance)
           (advance)
           (list :star qualifier)))
        (t (let ((tree (parse-expression))
                 (alias nil))
             (cond ((accept-word "AS") (setf alias (parse-name "a name after AS")))
                   ((and (eq (token-type (tok)) :word)
                         (or (token-quoted (tok)) (not (reserved-token-p (tok)))))
                    (setf alias (parse-name "a name"))))
             (list :expr tree alias)))))

(defun parse-sql-order-key ()
  (let ((tree (parse-expression))
        (direction :asc)
        (nulls nil))
    (cond ((accept-word "DESC") (setf direction :desc))
          ((accept-word "ASC")))
    (when (accept-word "NULLS")
      (cond ((accept-word "FIRST") (setf nulls :first))
            ((accept-word "LAST") (setf nulls :last))
            (t (parse-fail "Expected FIRST or LAST"))))
    (list tree direction nulls)))

(defun parse-comma-list (parser)
  (let ((items (list (funcall parser))))
    (loop while (accept-symbol ",") do (push (funcall parser) items))
    (nreverse items)))

(defun parse-alias ()
  (cond ((accept-word "AS") (parse-name "an alias after AS"))
        ((and (eq (token-type (tok)) :word)
              (or (token-quoted (tok)) (not (reserved-token-p (tok)))))
         (parse-name "an alias"))))

;; A table name and its alias, as (name quoted alias). In a FROM or a JOIN it can also be
;; a query in brackets with an alias, as (:derived query alias).
(defun parse-table-reference (&optional derived-ok)
  (if (and derived-ok (at-symbol "(") (at-word "SELECT" 1))
      (progn
        (advance)
        (let ((query (parse-query)))
          (expect-symbol ")")
          (let ((alias (parse-alias)))
            (unless alias
              (parse-fail "A query in FROM needs an alias"))
            (list :derived query alias))))
      (multiple-value-bind (name quoted) (parse-name "a table name")
        (list name quoted (parse-alias)))))

;; A join is (kind table-reference condition) with kind :inner, :left, :right or :full.
;; Return nil when the next words are not a join.
(defun parse-join ()
  (let ((kind (cond ((accept-word "INNER") :inner)
                    ((accept-word "LEFT") (accept-word "OUTER") :left)
                    ((accept-word "RIGHT") (accept-word "OUTER") :right)
                    ((accept-word "FULL") (accept-word "OUTER") :full)
                    ((or (at-word "CROSS") (at-word "NATURAL"))
                     (parse-fail "~A JOIN is not supported. Use JOIN ... ON a = b" (string-upcase (token-value (tok)))))
                    ((at-word "JOIN") :inner)
                    (t nil))))
    (when kind
      (expect-word "JOIN")
      (let ((table (parse-table-reference t)))
        (when (at-word "USING")
          (parse-fail "USING is not supported. Use ON a.x = b.x"))
        (unless (accept-word "ON")
          (parse-fail "Expected ON. A join needs a condition"))
        (list kind table (parse-expression))))))

;; A query: for now one SELECT. Set operations come next.
(defun parse-query ()
  (parse-select))

(defun parse-select ()
  (expect-word "SELECT")
  (when (or (at-word "UNION") (at-word "INTERSECT") (at-word "EXCEPT"))
    (parse-fail "Set operations are not supported"))
  (let* ((distinct (and (accept-word "DISTINCT") t))
         (items (parse-comma-list #'parse-select-item))
         from joins where group having order limit offset)
    (unless (accept-word "FROM")
      (parse-fail "Expected FROM. A SELECT without a table is not supported"))
    (setf from (parse-table-reference t))
    (loop for join = (parse-join) while join do (push join joins))
    (setf joins (nreverse joins))
    (when (accept-symbol ",")
      (parse-fail "A list of tables is not supported. Use JOIN ... ON"))
    (when (accept-word "WHERE") (setf where (parse-expression)))
    (when (accept-word "GROUP")
      (expect-word "BY")
      (setf group (parse-comma-list #'parse-expression)))
    (when (accept-word "HAVING") (setf having (parse-expression)))
    (when (accept-word "ORDER")
      (expect-word "BY")
      (setf order (parse-comma-list #'parse-sql-order-key)))
    (loop
      (cond ((and (null limit) (accept-word "LIMIT")) (setf limit (parse-expression)))
            ((and (null offset) (accept-word "OFFSET")) (setf offset (parse-expression)))
            (t (return))))
    (when (or (at-word "UNION") (at-word "INTERSECT") (at-word "EXCEPT"))
      (parse-fail "Set operations are not supported"))
    (list :select :distinct distinct :items items :from from :joins joins :where where
                  :group group :having having :order order :limit limit :offset offset)))

;; Parse TEXT, which holds one or more statements separated by semicolons. Return the
;; list of trees and the number of ? parameters in all of them.
(defun sql-parse (text)
  (with-sql-parser (text)
    (let ((statements '()))
      (loop
        (loop while (accept-symbol ";"))
        (when (at-end-p) (return))
        (push (parse-statement) statements)
        (unless (or (at-symbol ";") (at-end-p))
          (parse-fail "Unexpected text after the statement")))
      (unless statements
        (parse-fail "There is no statement"))
      (values (nreverse statements) *param-count*))))

;; INSERT gives (:insert table-reference columns rows select). COLUMNS is a list of names
;; or nil. ROWS is a list of rows of expressions, in which (:default) means the default.
;; SELECT is a SELECT tree instead of ROWS.
(defun parse-insert ()
  (expect-word "INSERT")
  (expect-word "INTO")
  (let ((table (parse-table-reference))
        (columns nil))
    (when (accept-symbol "(")
      (setf columns (parse-comma-list #'(lambda () (parse-name "a column name"))))
      (expect-symbol ")"))
    (cond ((accept-word "VALUES")
           (list :insert table columns
                 (parse-comma-list
                  #'(lambda ()
                      (expect-symbol "(")
                      (prog1 (parse-comma-list
                              #'(lambda ()
                                  (if (and (at-word "DEFAULT") (or (at-symbol "," 1) (at-symbol ")" 1)))
                                      (progn (advance) (list :default))
                                      (parse-expression))))
                        (expect-symbol ")"))))
                 nil))
          ((at-word "SELECT") (list :insert table columns nil (parse-select)))
          (t (parse-fail "Expected VALUES or SELECT")))))

;; UPDATE gives (:update table-reference ((column tree) ...) where).
(defun parse-update ()
  (expect-word "UPDATE")
  (let ((table (parse-table-reference)))
    (expect-word "SET")
    (let ((assignments (parse-comma-list
                        #'(lambda ()
                            (let ((column (parse-name "a column name")))
                              (expect-symbol "=")
                              (list column (parse-expression)))))))
      (list :update table assignments (and (accept-word "WHERE") (parse-expression))))))

;; DELETE gives (:delete table-reference where).
(defun parse-delete ()
  (expect-word "DELETE")
  (expect-word "FROM")
  (let ((table (parse-table-reference)))
    (list :delete table (and (accept-word "WHERE") (parse-expression)))))

;;; Definitions
;;;
;;; A column definition is (:column name type-name constraints). The type name is
;;; upper case, such as "INTEGER". A constraint is one of
;;;   (:not-null) (:null) (:primary-key) (:unique) (:default tree) (:check tree)
;;;   (:references table quoted column action)     ACTION is :cascade, :set-null or :restrict
;;; A constraint of the table is (:table-primary-key name) (:table-unique name)
;;; (:table-foreign-key name references) or (:table-check tree).
;;; CREATE TABLE gives (:create-table name quoted if-not-exists elements).

(defun parse-type-name ()
  (let ((word (string-upcase (parse-name "a column type"))))
    (cond ((and (string= word "DOUBLE") (accept-word "PRECISION")) "DOUBLE")
          ((and (string= word "CHARACTER") (accept-word "VARYING")) "VARCHAR")
          (t word))))

(defun parse-references ()
  (multiple-value-bind (table quoted) (parse-name "a table name")
    (let ((column nil) (action :restrict))
      (when (accept-symbol "(")
        (setf column (parse-name "a column name"))
        (expect-symbol ")"))
      (loop while (at-word "ON")
            do (advance)
               (cond ((accept-word "DELETE")
                      (cond ((accept-word "CASCADE") (setf action :cascade))
                            ((accept-word "SET") (expect-word "NULL") (setf action :set-null))
                            ((accept-word "RESTRICT") (setf action :restrict))
                            ((accept-word "NO") (expect-word "ACTION") (setf action :restrict))
                            (t (parse-fail "Expected CASCADE, SET NULL, RESTRICT or NO ACTION"))))
                     ((at-word "UPDATE") (parse-fail "ON UPDATE is not supported. Keys are not changed"))
                     (t (parse-fail "Expected DELETE"))))
      (list :references table quoted column action))))

(defun parse-column-constraints ()
  (let ((constraints '()))
    (loop
      (cond ((accept-word "NOT") (expect-word "NULL") (push (list :not-null) constraints))
            ((accept-word "NULL") (push (list :null) constraints))
            ((accept-word "PRIMARY") (expect-word "KEY") (accept-word "AUTOINCREMENT")
             (push (list :primary-key) constraints))
            ((accept-word "UNIQUE") (push (list :unique) constraints))
            ((accept-word "DEFAULT") (push (list :default (parse-additive)) constraints))
            ((accept-word "CHECK")
             (expect-symbol "(")
             (push (list :check (parse-expression)) constraints)
             (expect-symbol ")"))
            ((accept-word "REFERENCES") (push (parse-references) constraints))
            ((at-word "CONSTRAINT") (parse-fail "Named constraints are not supported"))
            (t (return))))
    (nreverse constraints)))

(defun parse-column-definition ()
  (let ((name (parse-name "a column name"))
        (type (parse-type-name)))
    (when (accept-symbol "(")
      (loop until (accept-symbol ")")
            do (unless (eq (token-type (advance)) :number)
                 (decf *pos*)
                 (parse-fail "Expected a number in the type"))
               (accept-symbol ",")))
    (list :column name type (parse-column-constraints))))

(defun parse-bracketed-name ()
  (expect-symbol "(")
  (let ((name (parse-name "a column name")))
    (when (at-symbol ",")
      (parse-fail "A key or a unique constraint of several columns is not supported"))
    (expect-symbol ")")
    name))

(defun parse-table-element ()
  (cond ((accept-word "PRIMARY") (expect-word "KEY") (list :table-primary-key (parse-bracketed-name)))
        ((accept-word "UNIQUE") (list :table-unique (parse-bracketed-name)))
        ((accept-word "FOREIGN")
         (expect-word "KEY")
         (let ((name (parse-bracketed-name)))
           (expect-word "REFERENCES")
           (list :table-foreign-key name (parse-references))))
        ((accept-word "CHECK")
         (expect-symbol "(")
         (prog1 (list :table-check (parse-expression)) (expect-symbol ")")))
        ((at-word "CONSTRAINT") (parse-fail "Named constraints are not supported"))
        (t (parse-column-definition))))

(defun parse-if-not-exists ()
  (and (accept-word "IF") (expect-word "NOT") (expect-word "EXISTS") t))

(defun parse-if-exists ()
  (and (accept-word "IF") (expect-word "EXISTS") t))

(defun parse-signed-integer ()
  (let ((sign (if (accept-symbol "-") -1 (progn (accept-symbol "+") 1)))
        (token (tok)))
    (unless (and (eq (token-type token) :number) (integerp (token-value token)))
      (parse-fail "Expected a whole number"))
    (advance)
    (* sign (token-value token))))

(defun parse-create ()
  (expect-word "CREATE")
  (let ((unique (accept-word "UNIQUE")))
    (cond
      ((and (not unique) (accept-word "TABLE"))
       (let ((if-not-exists (parse-if-not-exists)))
         (multiple-value-bind (name quoted) (parse-name "a table name")
           (expect-symbol "(")
           (let ((elements (parse-comma-list #'parse-table-element)))
             (expect-symbol ")")
             (list :create-table name quoted if-not-exists elements)))))
      ((accept-word "INDEX")
       (let* ((if-not-exists (parse-if-not-exists))
              (name (unless (at-word "ON") (parse-name "an index name"))))
         (expect-word "ON")
         (multiple-value-bind (table quoted) (parse-name "a table name")
           (let ((column (parse-bracketed-name)))
             (list :create-index name (and unique t) if-not-exists table quoted column)))))
      ((and (not unique) (accept-word "VIEW"))
       (let ((if-not-exists (parse-if-not-exists)))
         (multiple-value-bind (name quoted) (parse-name "a view name")
           (expect-word "AS")
           (let* ((before *param-count*)
                  (select (parse-select)))
             (unless (= before *param-count*)
               (parse-fail "A view can't have ? parameters. Write the values in the query"))
             (list :create-view name quoted if-not-exists select)))))
      ((and (not unique) (accept-word "SEQUENCE"))
       (let ((if-not-exists (parse-if-not-exists))
             (start 1) (increment 1))
         (multiple-value-bind (name quoted) (parse-name "a sequence name")
           (loop
             (cond ((accept-word "START") (accept-word "WITH") (setf start (parse-signed-integer)))
                   ((accept-word "INCREMENT") (accept-word "BY") (setf increment (parse-signed-integer)))
                   (t (return))))
           (list :create-sequence name quoted if-not-exists start increment))))
      (t (parse-fail "Expected TABLE, INDEX, VIEW or SEQUENCE after CREATE")))))

;; DROP gives (:drop kind name quoted if-exists table), with kind :table, :view, :index or
;; :sequence. TABLE is the table of DROP INDEX name ON table, or nil.
(defun parse-drop ()
  (expect-word "DROP")
  (let ((kind (cond ((accept-word "TABLE") :table)
                    ((accept-word "VIEW") :view)
                    ((accept-word "INDEX") :index)
                    ((accept-word "SEQUENCE") :sequence)
                    (t (parse-fail "Expected TABLE, INDEX, VIEW or SEQUENCE after DROP"))))
        (if-exists (parse-if-exists)))
    (multiple-value-bind (name quoted) (parse-name "a name")
      (let ((table (and (eq kind :index) (accept-word "ON") (parse-name "a table name"))))
        (list :drop kind name quoted if-exists table)))))

;; ALTER TABLE gives (:alter-table name quoted action), where ACTION is
;; (:add-column definition), (:drop-column name), (:rename-column old new) or
;; (:rename-table new).
(defun parse-alter ()
  (expect-word "ALTER")
  (expect-word "TABLE")
  (multiple-value-bind (name quoted) (parse-name "a table name")
    (list :alter-table name quoted
          (cond ((accept-word "ADD")
                 (accept-word "COLUMN")
                 (when (or (at-word "PRIMARY") (at-word "UNIQUE") (at-word "FOREIGN") (at-word "CHECK")
                           (at-word "CONSTRAINT"))
                   (parse-fail "ADD of a constraint is not supported. Add a column with its constraints"))
                 (list :add-column (parse-column-definition)))
                ((accept-word "DROP")
                 (accept-word "COLUMN")
                 (list :drop-column (parse-name "a column name")))
                ((accept-word "RENAME")
                 (cond ((accept-word "TO") (list :rename-table (parse-name "a table name")))
                       (t (accept-word "COLUMN")
                          (let ((old (parse-name "a column name")))
                            (expect-word "TO")
                            (list :rename-column old (parse-name "a column name"))))))
                (t (parse-fail "Expected ADD, DROP or RENAME"))))))

(defun parse-statement ()
  (cond ((at-word "SELECT") (parse-select))
        ((accept-word "EXPLAIN")
         (unless (at-word "SELECT")
           (parse-fail "EXPLAIN works for SELECT only"))
         (list :explain (parse-select)))
        ((at-word "INSERT") (parse-insert))
        ((at-word "UPDATE") (parse-update))
        ((at-word "DELETE") (parse-delete))
        ((at-word "CREATE") (parse-create))
        ((at-word "DROP") (parse-drop))
        ((at-word "ALTER") (parse-alter))
        (t (parse-fail "This statement is not supported"))))
