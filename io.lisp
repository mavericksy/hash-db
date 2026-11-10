;;;; Import and export: CSV and JSON.
;;;;
;;;;   (export-csv "people" "people.csv")            ; to a file, or to a stream
;;;;   (import-csv "people" "people.csv")            ; => number of rows added
;;;;   (export-json "people" "people.json")
;;;;   (import-json "people" "people.json")
;;;;
;;;; The table can be a table name, a view, or a table value from select. An import is
;;;; one transaction: every row goes in, or none. Ids are generated, so a value for a
;;;; primary key column in a file is ignored. A file is written to a temporary file
;;;; and renamed, so a failed export leaves the old file.

(in-package :hash-db)

(defun import-fail (line format-control &rest args)
  (error 'import-error :line line
                       :message (format nil "~@[Line ~D: ~]~?" line format-control args)))

;;; Files and streams

(defun source-text (source)
  (let ((text (if (streamp source)
                  (uiop:slurp-stream-string source)
                  (uiop:read-file-string source :external-format :utf-8))))
    ;; a byte order mark that some programs put in front of a file
    (if (and (plusp (length text)) (char= (char text 0) (code-char #xFEFF)))
        (subseq text 1)
        text)))

(defun call-with-destination (destination fn)
  (if (streamp destination)
      (funcall fn destination)
      (call-with-atomic-file
       destination
       #'(lambda (tmp)
           (with-open-file (out tmp :direction :output :if-exists :supersede
                                    :external-format :utf-8)
             (funcall fn out))))))

;;; Values as text

(defun column-type-name (column)
  "The type name given when the column was made, such as NUMBER. Nil for a result column."
  (second (spec column)))

(defun float-text (x)
  (let ((*read-default-float-format* 'double-float))
    (prin1-to-string (coerce x 'double-float))))

(defun lower-name (keyword)
  (string-downcase (symbol-name keyword)))

;; The columns to write, as column objects.
(defun export-columns (tbl columns)
  (if columns
      (mapcar #'(lambda (name) (find-column name (schema tbl))) (if (listp columns) columns (list columns)))
      (schema tbl)))

;;; CSV

(defun csv-needs-quotes-p (string)
  (or (zerop (length string))
      (find-if #'(lambda (c) (member c '(#\, #\" #\Return #\Newline))) string)))

(defun write-csv-string (string out)
  (if (csv-needs-quotes-p string)
      (progn (write-char #\" out)
             (loop for c across string
                   do (when (char= c #\") (write-char #\" out))
                      (write-char c out))
             (write-char #\" out))
      (write-string string out)))

(defun write-csv-value (value out)
  (cond ((null-value-p value))
        ((stringp value) (write-csv-string value out))
        ((integerp value) (format out "~D" value))
        ((floatp value) (write-string (float-text value) out))
        ((rationalp value) (format out "~D/~D" (numerator value) (denominator value)))
        ((eq value t) (write-string "true" out))
        ((null value) (write-string "false" out))
        ((keywordp value) (write-csv-string (lower-name value) out))
        (t (error 'import-error :message (format nil "Can't write ~S as CSV" value)))))

;; Write the rows of TABLE as CSV, one line per row, with the column names on the first
;; line unless :header is nil. :columns picks and orders the columns. NULL is an empty
;; field and an empty string is "". Lines end with a newline. Return the number of rows.
(defun export-csv (table destination &key columns (header t))
  (let* ((tbl (find-table table))
         (cols (export-columns tbl columns))
         (count 0))
    (call-with-destination
     destination
     #'(lambda (out)
         (when header
           (loop for col in cols
                 for first = t then nil
                 do (unless first (write-char #\, out))
                    (write-csv-string (lower-name (name col)) out))
           (write-char #\Newline out))
         (dolist (row (rows-list (rows tbl)))
           (loop for col in cols
                 for first = t then nil
                 do (unless first (write-char #\, out))
                    (write-csv-value (getf row (name col)) out))
           (write-char #\Newline out)
           (incf count))))
    count))

;; Split CSV text into records. Return a list of (line . fields), where a field is
;; (text . quoted-p). A quoted field can hold commas, quotes ("") and line breaks.
;; A blank line is skipped. LF, CRLF and CR end a line.
(defun parse-csv (text)
  (let ((pos 0) (end (length text)) (line 1) (records '()))
    (labels ((peek () (and (< pos end) (char text pos)))
             (next () (prog1 (char text pos) (incf pos)))
             (end-of-line-p (c) (member c '(#\Newline #\Return)))
             (skip-line-end ()
               (let ((c (next)))
                 (when (and (char= c #\Return) (eql (peek) #\Newline)) (incf pos))
                 (incf line)))
             (quoted-field (start-line)
               (incf pos)
               (let ((out (make-string-output-stream)))
                 (loop
                   (let ((c (peek)))
                     (cond ((null c) (import-fail start-line "A quoted field is not closed"))
                           ((char= c #\")
                            (incf pos)
                            (if (eql (peek) #\")
                                (progn (write-char #\" out) (incf pos))
                                (return)))
                           (t (when (char= c #\Newline) (incf line))
                              (write-char (next) out)))))
                 (let ((c (peek)))
                   (unless (or (null c) (char= c #\,) (end-of-line-p c))
                     (import-fail start-line "Text after a closing quote")))
                 (cons (get-output-stream-string out) t)))
             (plain-field ()
               (let ((start pos))
                 (loop for c = (peek)
                       while (and c (char/= c #\,) (not (end-of-line-p c)))
                       do (when (char= c #\") (import-fail line "A quote inside an unquoted field"))
                          (incf pos))
                 (cons (subseq text start pos) nil))))
      (loop while (< pos end)
            do (let ((start-line line) (fields '()))
                 (loop
                   (push (if (eql (peek) #\") (quoted-field start-line) (plain-field)) fields)
                   (let ((c (peek)))
                     (cond ((null c) (return))
                           ((char= c #\,) (incf pos))
                           (t (skip-line-end) (return)))))
                 (setf fields (nreverse fields))
                 (unless (and (null (rest fields)) (equal (car (first fields)) "") (not (cdr (first fields))))
                   (push (cons start-line fields) records))))
      (nreverse records))))

;;; Text to values

(defun parse-number-text (text)
  (let ((text (string-trim " " text)))
    (let ((slash (position #\/ text)))
      (if slash
          (let ((n (ignore-errors (parse-integer text :end slash)))
                (d (ignore-errors (parse-integer text :start (1+ slash)))))
            (and n d (plusp d) (/ n d)))
          (let ((value (handler-case (json-decode text) (json-error () nil))))
            (and (realp value) value))))))

(defun parse-boolean-text (text)
  (let ((text (string-downcase (string-trim " " text))))
    (cond ((member text '("true" "t" "yes" "1") :test #'string=) :true)
          ((member text '("false" "f" "no" "0") :test #'string=) :false))))

;; The value for one CSV field of COL.
(defun csv-field-value (field col line)
  (destructuring-bind (text . quoted) field
    (let ((type (column-type-name col)))
      (cond ((and (string= text "") (not quoted)) :null)
            ((and (string= text "") (not (member type '(string interned-string json any))))
             :null)
            (t
             (case type
               ((string interned-string json any) text)
               ((integer timestamp foreign-key-column)
                (or (ignore-errors (parse-integer (string-trim " " text)))
                    (import-fail line "~S is not a whole number (column ~A)" text (name col))))
               ((number float)
                (or (parse-number-text text)
                    (import-fail line "~S is not a number (column ~A)" text (name col))))
               (boolean
                (let ((value (parse-boolean-text text)))
                  (cond ((eq value :true) t)
                        ((eq value :false) nil)
                        (t (import-fail line "~S is not true or false (column ~A)" text (name col))))))
               (keyword (as-keyword (string-left-trim ":" (string-trim " " text))))
               (t text)))))))

;; The columns that a file of rows fills, as column objects. Ids are generated, so a primary
;; key column is left out of the default.
(defun import-columns (tbl names)
  (mapcar #'(lambda (name) (find-column name (schema tbl))) names))

(defun insert-imported-row (tbl pairs line)
  (handler-case (insert-row pairs (name tbl))
    (hash-db-error (c)
      (import-fail line "~A" (error-message c)))))

;; Read rows from a CSV file or stream into TABLE and return how many were added. With
;; :header t (the default) the first line names the columns. :columns names the columns in
;; file order instead, and the first line is then skipped when :header is t. Without both,
;; the fields are all the columns but the primary key, in order. An empty field is NULL.
;; The rows go in as one transaction.
(defun import-csv (table source &key columns (header t))
  (let* ((records (parse-csv (source-text source)))
         (header-record (and header (pop records))))
    (when (and header (null header-record))
      (import-fail nil "The file is empty, so it has no header line"))
    (with-transaction
      (let* ((tbl (stored-table table))
             (names (cond (columns (if (listp columns) columns (list columns)))
                          (header-record
                           (mapcar #'(lambda (field)
                                       (as-keyword (string-trim " " (car field))))
                                   (rest header-record)))
                          (t (loop for col in (schema tbl)
                                   unless (typep col 'primary-key-column) collect (name col)))))
             (cols (handler-case (import-columns tbl names)
                     (no-such-column (c)
                       (import-fail (and header-record (car header-record)) "~A" (error-message c)))))
             (count 0))
        (unless (= (length names) (length (remove-duplicates names)))
          (import-fail (and header-record (car header-record)) "A column is named twice: ~S" names))
        (dolist (record records)
          (destructuring-bind (line . fields) record
            (unless (= (length fields) (length cols))
              (import-fail line "~D fields, but ~D columns" (length fields) (length cols)))
            (insert-imported-row
             tbl
             (loop for field in fields
                   for col in cols
                   unless (typep col 'primary-key-column)
                     append (list (name col) (csv-field-value field col line)))
             line)
            (incf count)))
        count))))

;;; JSON

(defun value-to-json (value col)
  (cond ((null-value-p value) :null)
        ((eq value t) :true)
        ((null value) :false)
        ((keywordp value) (lower-name value))
        ((and col (eq (column-type-name col) 'json)) (json-decode value))
        ((rationalp value) (if (integerp value) value (coerce value 'double-float)))
        (t value)))

;; Write the rows of TABLE as one JSON array of objects, one object per line. The keys are the
;; column names in lower case. NULL is null, a keyword is a string, and the text of a json column
;; is written as JSON, not as a string. A fraction such as 1/2 is written as a decimal.
;; Return the number of rows.
(defun export-json (table destination &key columns)
  (let* ((tbl (find-table table))
         (cols (export-columns tbl columns))
         (rows (rows-list (rows tbl))))
    (call-with-destination
     destination
     #'(lambda (out)
         (write-char #\[ out)
         (loop for row in rows
               for first = t then nil
               do (unless first (write-char #\, out))
                  (write-char #\Newline out)
                  (json-write (cons :object
                                    (loop for col in cols
                                          collect (cons (lower-name (name col))
                                                        (value-to-json (getf row (name col)) col))))
                              out))
         (when rows (write-char #\Newline out))
         (write-char #\] out)
         (write-char #\Newline out)))
    (length rows)))

(defun json-to-value (data col line)
  (let ((type (column-type-name col)))
    (cond ((eq data :null) :null)
          ((eq data :true) t)
          ((eq data :false) nil)
          ((eq type 'json) (json-encode data))
          ((and (eq type 'keyword) (stringp data)) (as-keyword (string-left-trim ":" data)))
          ((or (consp data) (and (vectorp data) (not (stringp data))))
           (import-fail line "An object or an array can only go in a json column (column ~A)" (name col)))
          (t data))))

;; Read an array of objects from a JSON file or stream into TABLE and return how many rows
;; were added. Each key names a column, in any case. Missing keys give the column default.
;; The rows go in as one transaction.
(defun import-json (table source)
  (let ((data (handler-case (json-decode (source-text source))
                (json-error (c) (import-fail nil "~A" (error-message c))))))
    (unless (and (vectorp data) (not (stringp data)))
      (import-fail nil "The file must hold an array of objects"))
    (with-transaction
      (let ((tbl (stored-table table))
            (count 0))
        (loop for item across data
              for line from 1
              do (unless (and (consp item) (eq (first item) :object))
                   (import-fail line "Row ~D is not an object" line))
                 (insert-imported-row
                  tbl
                  (loop for (key . value) in (rest item)
                        for col = (handler-case (find-column (as-keyword key) (schema tbl))
                                    (no-such-column (c) (import-fail line "~A" (error-message c))))
                        unless (typep col 'primary-key-column)
                          append (list (name col) (json-to-value value col line)))
                  line)
                 (incf count))
        count))))
