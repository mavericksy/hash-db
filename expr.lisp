;;;; Expressions. An expression is plain data that compiles to a function of one
;;;; row. Queries use it for :where, and later for computed columns and sorting.
;;;;
;;;;   :age                column
;;;;   :null               NULL
;;;;   12  "x"  t  nil     constants. (quote :red) is a constant keyword.
;;;;   (= a b) (/= a b) (< a b) (> a b) (<= a b) (>= a b)
;;;;   (and ...) (or ...) (not a)
;;;;   (in a b c ...) (between a lo hi) (like a "pat%")
;;;;   (is-null a) (is-not-null a) (coalesce a b ...)
;;;;   (+ a b ...) (- a b) (* a b ...) (/ a b)
;;;;   (lower a) (upper a) (length a) (concat a b ...)
;;;;   (fn function a b ...)   call your own Lisp function with the values
;;;;
;;;; Operators match by name, so (like ...) and (:like ...) both work.
;;;;
;;;; Truth has three values, as in SQL: true, false and unknown. Comparing with
;;;; NULL gives unknown. A :where keeps a row only when the result is true.

(in-package :hash-db)

;;; Values

;; :lt, :eq or :gt. Real numbers, strings, keywords and booleans compare with
;; their own kind. Anything else is a query-error.
(defun value-compare (a b)
  (flet ((order (less greater)
           (cond (less :lt) (greater :gt) (t :eq))))
    (cond ((and (realp a) (realp b)) (order (< a b) (> a b)))
          ((and (stringp a) (stringp b)) (order (string< a b) (string> a b)))
          ((and (keywordp a) (keywordp b)) (order (string< a b) (string> a b)))
          ((and (typep a 'boolean) (typep b 'boolean)) (order (and (not a) b) (and a (not b))))
          (t (error 'query-error
                    :message (format nil "Can't compare ~S with ~S" a b))))))

(defun value-less (a b)
  (eq (value-compare a b) :lt))

;; Equal values of different kinds are simply not equal.
(defun value-equal (a b)
  (cond ((and (realp a) (realp b)) (= a b))
        ((and (stringp a) (stringp b)) (string= a b))
        (t (equal a b))))

;;; Three-valued truth: t, nil or :unknown

;; An unknown truth value acts like NULL when it is used as a value.
(defun nullish-p (value)
  (or (null-value-p value) (eq value :unknown)))

;; The value to store or show: unknown becomes NULL.
(defun materialize (value)
  (if (eq value :unknown) :null value))

(defun truth (value)
  (cond ((eq value t) t)
        ((null value) nil)
        ((or (null-value-p value) (eq value :unknown)) :unknown)
        (t t)))

;;; Compiler

;; SCHEMA is the columns an expression can name. ALIASES are result names
;; that an expression can name too, such as in ORDER-BY. Each is (name function column).
(defstruct (ectx (:constructor make-ectx (schema &optional aliases)))
  schema
  aliases)

(defvar *expr-ops* (make-hash-table :test 'equal))

(defmacro define-op (names (args ctx) &body body)
  `(let ((compiler (lambda (,args ,ctx) (declare (ignorable ,ctx)) ,@body)))
     (dolist (name ',(if (listp names) names (list names)))
       (setf (gethash name *expr-ops*) compiler))))

(defun bad-expression (format-control &rest args)
  (error 'query-error :message (apply #'format nil format-control args)))

(defun check-arity (name args min &optional (max min))
  (let ((n (length args)))
    (when (or (< n min) (and max (> n max)))
      (bad-expression "~A takes ~A argument~:P, got ~D" name
                      (cond ((null max) (format nil "~D or more" min))
                            ((= min max) min)
                            (t (format nil "~D to ~D" min max)))
                      n))))

(defun compile-expr (expr ctx)
  (cond ((eq expr :null) (constantly :null))
        ((keywordp expr) (compile-column-ref expr ctx))
        ((or (null expr) (eq expr t)) (constantly expr))
        ((symbolp expr) (bad-expression "Unknown name ~S. Columns are keywords, such as :~A" expr expr))
        ((atom expr) (constantly expr))
        (t (compile-call expr ctx))))

;; Compile an expression whose result is stored or sorted. Unknown becomes NULL.
(defun compile-value (expr ctx)
  (let ((f (compile-expr expr ctx)))
    #'(lambda (row) (materialize (funcall f row)))))

(defun compile-column-ref (keyword ctx)
  (let ((alias (find keyword (ectx-aliases ctx) :key #'first)))
    (if alias
        (second alias)
        (let ((name (name (find-column keyword (ectx-schema ctx)))))
          #'(lambda (row) (getf row name))))))

(defun compile-call (expr ctx)
  (let ((op (first expr)))
    (unless (symbolp op)
      (bad-expression "Not an expression: ~S" expr))
    (let ((compiler (gethash (symbol-name op) *expr-ops*)))
      (unless compiler
        (bad-expression "Unknown operator ~S in ~S" op expr))
      (funcall compiler (rest expr) ctx))))

(defun compile-args (args ctx)
  (mapcar #'(lambda (arg) (compile-expr arg ctx)) args))

(defmacro call-all (fns row)
  `(mapcar #'(lambda (f) (funcall f ,row)) ,fns))

;; Compile a :where. It can be a function of one row, the name of such a
;; function, or an expression. Return a function of one row that is true when
;; the row passes, or nil when there is no condition.
(defun compile-where (where schema)
  (cond ((null where) nil)
        ((functionp where) where)
        ((and (symbolp where) (not (keywordp where)) (not (eq where t)))
         (if (fboundp where)
             (fdefinition where)
             (bad-expression "~S is not a function" where)))
        (t (let ((f (compile-expr where (make-ectx schema))))
             #'(lambda (row) (eq (truth (funcall f row)) t))))))

;;; Constants and logic

(define-op "QUOTE" (args ctx)
  (check-arity "QUOTE" args 1)
  (constantly (first args)))

(define-op "AND" (args ctx)
  (check-arity "AND" args 1 nil)
  (let ((fns (compile-args args ctx)))
    #'(lambda (row)
        (let ((result t))
          (dolist (f fns result)
            (let ((v (truth (funcall f row))))
              (cond ((null v) (return nil))
                    ((eq v :unknown) (setf result :unknown)))))))))

(define-op "OR" (args ctx)
  (check-arity "OR" args 1 nil)
  (let ((fns (compile-args args ctx)))
    #'(lambda (row)
        (let ((result nil))
          (dolist (f fns result)
            (let ((v (truth (funcall f row))))
              (cond ((eq v t) (return t))
                    ((eq v :unknown) (setf result :unknown)))))))))

(define-op "NOT" (args ctx)
  (check-arity "NOT" args 1)
  (let ((f (compile-expr (first args) ctx)))
    #'(lambda (row)
        (let ((v (truth (funcall f row))))
          (cond ((eq v t) nil)
                ((null v) t)
                (t :unknown))))))

;;; Comparison

(defmacro define-comparison (name test)
  `(define-op ,name (args ctx)
     (check-arity ,name args 2)
     (destructuring-bind (fa fb) (compile-args args ctx)
       #'(lambda (row)
           (let ((a (funcall fa row))
                 (b (funcall fb row)))
             (if (or (nullish-p a) (nullish-p b))
                 :unknown
                 (let ((order (value-compare a b)))
                   (declare (ignorable order))
                   ,test)))))))

(define-op "=" (args ctx)
  (check-arity "=" args 2)
  (destructuring-bind (fa fb) (compile-args args ctx)
    #'(lambda (row)
        (let ((a (funcall fa row)) (b (funcall fb row)))
          (if (or (nullish-p a) (nullish-p b)) :unknown (value-equal a b))))))

(define-op "/=" (args ctx)
  (check-arity "/=" args 2)
  (destructuring-bind (fa fb) (compile-args args ctx)
    #'(lambda (row)
        (let ((a (funcall fa row)) (b (funcall fb row)))
          (if (or (nullish-p a) (nullish-p b)) :unknown (not (value-equal a b)))))))

(define-comparison "<"  (eq order :lt))
(define-comparison ">"  (eq order :gt))
(define-comparison "<=" (not (eq order :gt)))
(define-comparison ">=" (not (eq order :lt)))

(define-op "IN" (args ctx)
  (check-arity "IN" args 2 nil)
  (destructuring-bind (fx &rest fvs) (compile-args args ctx)
    #'(lambda (row)
        (let ((x (funcall fx row)))
          (if (nullish-p x)
              :unknown
              (let ((unknown nil))
                (dolist (f fvs (if unknown :unknown nil))
                  (let ((v (funcall f row)))
                    (cond ((nullish-p v) (setf unknown t))
                          ((value-equal x v) (return t)))))))))))

(define-op "BETWEEN" (args ctx)
  (check-arity "BETWEEN" args 3)
  (destructuring-bind (fx flo fhi) (compile-args args ctx)
    #'(lambda (row)
        (let ((x (funcall fx row)) (lo (funcall flo row)) (hi (funcall fhi row)))
          ;; the same as (and (>= x lo) (<= x hi)), with unknown for NULL
          (let ((above (if (or (nullish-p x) (nullish-p lo)) :unknown
                           (not (eq (value-compare x lo) :lt))))
                (below (if (or (nullish-p x) (nullish-p hi)) :unknown
                           (not (eq (value-compare x hi) :gt)))))
            (cond ((or (null above) (null below)) nil)
                  ((or (eq above :unknown) (eq below :unknown)) :unknown)
                  (t t)))))))

(define-op "IS-NULL" (args ctx)
  (check-arity "IS-NULL" args 1)
  (let ((f (compile-expr (first args) ctx)))
    #'(lambda (row) (nullish-p (funcall f row)))))

(define-op "IS-NOT-NULL" (args ctx)
  (check-arity "IS-NOT-NULL" args 1)
  (let ((f (compile-expr (first args) ctx)))
    #'(lambda (row) (not (nullish-p (funcall f row))))))

;;; LIKE. % matches any run of characters, _ matches one character,
;;; and a backslash makes the next character plain.

(defun like-tokens (pattern)
  (let ((tokens '()) (i 0) (n (length pattern)))
    (loop while (< i n)
          do (let ((c (char pattern i)))
               (cond ((char= c #\%) (push :run tokens))
                     ((char= c #\_) (push :char tokens))
                     ((and (char= c #\\) (< (1+ i) n))
                      (incf i)
                      (push (char pattern i) tokens))
                     (t (push c tokens)))
               (incf i)))
    (coerce (nreverse tokens) 'vector)))

(defun like-match-p (string tokens)
  (let* ((n (length string))
         (cur (make-array (1+ n) :initial-element nil)))
    (setf (aref cur 0) t)
    (loop for token across tokens
          do (let ((next (make-array (1+ n) :initial-element nil)))
               (if (eq token :run)
                   (let ((seen nil))
                     (dotimes (i (1+ n))
                       (when (aref cur i) (setf seen t))
                       (setf (aref next i) seen)))
                   (loop for i from 1 to n
                         when (and (aref cur (1- i))
                                   (or (eq token :char) (char= token (char string (1- i)))))
                           do (setf (aref next i) t)))
               (setf cur next)))
    (aref cur n)))

(define-op "LIKE" (args ctx)
  (check-arity "LIKE" args 2)
  (destructuring-bind (fx fp) (compile-args args ctx)
    #'(lambda (row)
        (let ((x (funcall fx row)) (p (funcall fp row)))
          (cond ((or (nullish-p x) (nullish-p p)) :unknown)
                ((and (stringp x) (stringp p)) (like-match-p x (like-tokens p)))
                (t (bad-expression "LIKE needs strings, got ~S and ~S" x p)))))))

;;; Arithmetic and strings. NULL in gives NULL out.

(defun require-real (value op)
  (unless (realp value)
    (bad-expression "~A needs numbers, got ~S" op value))
  value)

(defun require-string (value op)
  (unless (stringp value)
    (bad-expression "~A needs strings, got ~S" op value))
  value)

(defmacro define-arithmetic (name min max (values) &body body)
  `(define-op ,name (args ctx)
     (check-arity ,name args ,min ,max)
     (let ((fns (compile-args args ctx)))
       #'(lambda (row)
           (let ((,values (call-all fns row)))
             (if (some #'nullish-p ,values)
                 :null
                 (progn (dolist (v ,values) (require-real v ,name))
                        ,@body)))))))

(define-arithmetic "+" 1 nil (vs) (apply #'+ vs))
(define-arithmetic "*" 1 nil (vs) (apply #'* vs))
(define-arithmetic "-" 1 nil (vs) (apply #'- vs))
(define-arithmetic "/" 2 2 (vs)
  (if (zerop (second vs))
      (bad-expression "Division by zero")
      (apply #'/ vs)))

(defmacro define-string-fn (name (s) &body body)
  `(define-op ,name (args ctx)
     (check-arity ,name args 1)
     (let ((f (compile-expr (first args) ctx)))
       #'(lambda (row)
           (let ((,s (funcall f row)))
             (if (nullish-p ,s)
                 :null
                 (progn (require-string ,s ,name) ,@body)))))))

(define-string-fn "LOWER" (s) (string-downcase s))
(define-string-fn "UPPER" (s) (string-upcase s))
(define-string-fn "LENGTH" (s) (length s))

(define-op "CONCAT" (args ctx)
  (check-arity "CONCAT" args 1 nil)
  (let ((fns (compile-args args ctx)))
    #'(lambda (row)
        (let ((vs (call-all fns row)))
          (if (some #'nullish-p vs)
              :null
              (apply #'concatenate 'string
                     (mapcar #'(lambda (v) (require-string v "CONCAT")) vs)))))))

(define-op "COALESCE" (args ctx)
  (check-arity "COALESCE" args 1 nil)
  (let ((fns (compile-args args ctx)))
    #'(lambda (row)
        (dolist (f fns :null)
          (let ((v (funcall f row)))
            (unless (nullish-p v) (return v)))))))

;;; Your own functions

(define-op "FN" (args ctx)
  (check-arity "FN" args 1 nil)
  (let ((function (first args)))
    (unless (or (functionp function) (and (symbolp function) function (fboundp function)))
      (bad-expression "FN needs a function or the name of one, got ~S" function))
    (let ((fns (compile-args (rest args) ctx)))
      #'(lambda (row)
          (apply function (mapcar #'materialize (call-all fns row)))))))
