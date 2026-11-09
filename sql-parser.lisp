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
            (when (at-word "SELECT")
              (parse-fail "Subqueries are not supported"))
            (let ((items (list (parse-expression))))
              (loop while (accept-symbol ",") do (push (parse-expression) items))
              (expect-symbol ")")
              (list :in left (nreverse items) negated)))
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
              (when (at-word "SELECT")
                (parse-fail "Subqueries are not supported"))
              (prog1 (parse-expression) (expect-symbol ")")))
             (t (parse-fail "Expected an expression"))))
      (:word
       (cond ((token-quoted token) (parse-column-reference))
             ((at-word "NULL") (advance) (list :lit :null))
             ((at-word "TRUE") (advance) (list :lit t))
             ((at-word "FALSE") (advance) (list :lit nil))
             ((at-word "CASE") (parse-fail "CASE is not supported"))
             ((at-word "CURRENT_TIMESTAMP") (advance) (list :call "NOW" nil nil nil))
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

;; Parse TEXT as one expression and nothing else.
(defun sql-parse-expression (text)
  (with-sql-parser (text)
    (let ((ast (parse-expression)))
      (accept-symbol ";")
      (unless (at-end-p)
        (parse-fail "Unexpected text after the expression"))
      (values ast *param-count*))))
