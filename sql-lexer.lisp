;;;; SQL: the tokenizer.
;;;;
;;;; SQL text becomes a vector of tokens. A token has a type, a value and the place
;;;; in the text where it starts.
;;;;   :word     a name or a keyword. The value is the text as written.
;;;;             QUOTED is true for "double quoted" names.
;;;;   :string   'text'. The value is the text, with '' read as one quote.
;;;;   :number   an integer, or a double-float when it has a point or an exponent.
;;;;   :param    ?
;;;;   :symbol   ( ) , . * + - / % = <> != < > <= >= || ;   The value is a string.
;;;;   :eof      the end. The last token.
;;;; Comments, -- to the end of the line and /* ... */, are skipped.

(in-package :hash-db)

(defstruct (token (:constructor make-token (type value position &optional quoted)))
  type
  value
  position
  quoted)

;; "line 2, column 7, near \"SELECT x FRM\"" for the place POSITION in TEXT.
(defun describe-position (text position)
  (let* ((position (min position (length text)))
         (line (1+ (count #\Newline text :end position)))
         (line-start (let ((nl (position #\Newline text :end position :from-end t)))
                       (if nl (1+ nl) 0)))
         (column (1+ (- position line-start)))
         (near (subseq text position (min (length text) (+ position 24)))))
    (format nil "line ~D, column ~D~@[, near ~S~]" line column
            (and (plusp (length near)) (substitute #\Space #\Newline near)))))

(defun sql-fail (text position format-control &rest args)
  (error 'sql-error
         :position position
         :sql text
         :message (format nil "SQL error~@[ at ~A~]: ~?"
                          (and position (describe-position text position))
                          format-control args)))

(defun word-start-char-p (c)
  (or (alpha-char-p c) (char= c #\_)))

(defun word-char-p (c)
  (or (alphanumericp c) (char= c #\_) (char= c #\$)))

(defun sql-tokenize (text)
  "Split SQL text into a vector of tokens."
  (let ((tokens '())
        (pos 0)
        (end (length text)))
    (labels ((peek (&optional (n 0))
               (and (< (+ pos n) end) (char text (+ pos n))))
             (emit (type value start &optional quoted)
               (push (make-token type value start quoted) tokens))
             (quoted (quote-char start)
               ;; read to the closing quote. A doubled quote is one quote in the text.
               (incf pos)
               (let ((out (make-string-output-stream)))
                 (loop
                   (let ((c (peek)))
                     (cond ((null c)
                            (sql-fail text start "A ~A is not closed"
                                      (if (char= quote-char #\') "string" "quoted name")))
                           ((char= c quote-char)
                            (cond ((eql (peek 1) quote-char) (write-char c out) (incf pos 2))
                                  (t (incf pos) (return))))
                           (t (write-char c out) (incf pos)))))
                 (get-output-stream-string out)))
             (number-token (start)
               (loop while (and (peek) (digit-char-p (peek))) do (incf pos))
               (let ((fraction nil))
                 (when (and (eql (peek) #\.) (peek 1) (digit-char-p (peek 1)))
                   (setf fraction t)
                   (incf pos)
                   (loop while (and (peek) (digit-char-p (peek))) do (incf pos)))
                 (when (and (member (peek) '(#\e #\E))
                            (or (and (peek 1) (digit-char-p (peek 1)))
                                (and (member (peek 1) '(#\+ #\-)) (peek 2) (digit-char-p (peek 2)))))
                   (setf fraction t)
                   (incf pos 2)
                   (loop while (and (peek) (digit-char-p (peek))) do (incf pos)))
                 (let ((digits (subseq text start pos)))
                   (when (char= (char digits 0) #\.)
                     (setf digits (concatenate 'string "0" digits)))
                   (emit :number
                         (if fraction (json-decode digits) (parse-integer digits))
                         start)))))
      (loop
        (let ((c (peek)))
          (cond
            ((null c) (return))
            ((member c '(#\Space #\Tab #\Newline #\Return #\Page)) (incf pos))
            ((and (char= c #\-) (eql (peek 1) #\-))
             (loop while (and (peek) (char/= (peek) #\Newline)) do (incf pos)))
            ((and (char= c #\/) (eql (peek 1) #\*))
             (let ((close (search "*/" text :start2 (+ pos 2))))
               (unless close (sql-fail text pos "A comment is not closed"))
               (setf pos (+ close 2))))
            ((word-start-char-p c)
             (let ((start pos))
               (loop while (and (peek) (word-char-p (peek))) do (incf pos))
               (emit :word (subseq text start pos) start)))
            ((char= c #\") (let ((start pos)) (emit :word (quoted #\" start) start t)))
            ((char= c #\') (let ((start pos)) (emit :string (quoted #\' start) start)))
            ((or (digit-char-p c) (and (char= c #\.) (peek 1) (digit-char-p (peek 1))))
             (number-token pos))
            ((char= c #\?) (emit :param nil pos) (incf pos))
            ((and (member c '(#\< #\> #\! #\|)) (peek 1)
                  (member (subseq text pos (+ pos 2)) '("<>" "<=" ">=" "!=" "||") :test #'string=))
             (emit :symbol (subseq text pos (+ pos 2)) pos)
             (incf pos 2))
            ((find c "(),.*+-/%=<>;")
             (emit :symbol (string c) pos)
             (incf pos))
            (t (sql-fail text pos "Unexpected character ~S" c)))))
      (emit :eof nil end)
      (coerce (nreverse tokens) 'simple-vector))))
