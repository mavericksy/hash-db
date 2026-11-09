(in-package #:hash-db-tests)

(in-suite hash-db-suite)

(defun token-summary (text)
  "The tokens of TEXT as (type value) pairs, without the end token."
  (loop for token across (hash-db::sql-tokenize text)
        unless (eq (hash-db::token-type token) :eof)
          collect (list (hash-db::token-type token) (hash-db::token-value token))))

(def-test lexer-splits-words-and-symbols (:fixture clean-db)
  (is (equal '((:word "SELECT") (:word "a") (:symbol ",") (:word "b_2") (:word "FROM") (:word "t$1")
               (:symbol ";"))
             (token-summary "SELECT a, b_2 FROM t$1;")))
  (is (equal '((:symbol "(") (:symbol ")") (:symbol "*") (:symbol "+") (:symbol "-") (:symbol "/")
               (:symbol "%") (:symbol "=") (:symbol ".") (:symbol ","))
             (token-summary "()*+-/%=.,")))
  (is (equal '((:symbol "<>") (:symbol "!=") (:symbol "<=") (:symbol ">=") (:symbol "||")
               (:symbol "<") (:symbol ">"))
             (token-summary "<> != <= >= || < >")))
  (is (equal '((:word "a") (:symbol "<=") (:word "b")) (token-summary "a<=b"))))

(def-test lexer-reads-numbers (:fixture clean-db)
  (is (equal '((:number 0) (:number 42) (:number 1234567890123456789012)) 
             (token-summary "0 42 1234567890123456789012")))
  (is (equal '((:number 1.5d0) (:number 0.5d0) (:number 1000.0d0) (:number 0.015d0) (:number 250.0d0))
             (token-summary "1.5 .5 1e3 1.5E-2 2.5e+2")))
  (is (equal '((:number 1) (:symbol ".") (:word "x")) (token-summary "1.x")) "a point needs a digit after it")
  (is (equal '((:number 1) (:word "e")) (token-summary "1e")) "an exponent needs digits")
  (is (equal '((:symbol "-") (:number 5)) (token-summary "-5")) "the sign is a symbol"))

(def-test lexer-reads-strings-and-quoted-names (:fixture clean-db)
  (is (equal '((:string "hi") (:string "it's") (:string "") (:string "a
b")) (token-summary "'hi' 'it''s' '' 'a
b'")))
  (is (equal '((:string "-- not a comment") (:string "/* nor this */")) (token-summary "'-- not a comment' '/* nor this */'")))
  (let ((tokens (hash-db::sql-tokenize "\"My Table\" \"a\"\"b\" plain")))
    (is (equal '("My Table" "a\"b" "plain") (map 'list #'hash-db::token-value (subseq tokens 0 3))))
    (is (equal '(t t nil) (map 'list #'hash-db::token-quoted (subseq tokens 0 3))))))

(def-test lexer-skips-comments (:fixture clean-db)
  (is (equal '((:word "a") (:word "b")) (token-summary "a -- the rest is a comment
b")))
  (is (equal '((:word "a") (:word "b")) (token-summary "a /* in
side */ b")))
  (is (equal '((:word "a")) (token-summary "a -- ends the text")))
  (is (equal '((:symbol "-") (:symbol "-")) (token-summary "- -")) "two minus signs apart are not a comment"))

(def-test lexer-reads-parameters (:fixture clean-db)
  (is (equal '((:word "a") (:symbol "=") (:param nil) (:word "AND") (:word "b") (:symbol ">") (:param nil))
             (token-summary "a = ? AND b > ?"))))

(def-test lexer-positions-and-end (:fixture clean-db)
  (let ((tokens (hash-db::sql-tokenize "ab  cd")))
    (is (equal '(0 4 6) (map 'list #'hash-db::token-position tokens)))
    (is (eq :eof (hash-db::token-type (aref tokens 2)))))
  (is (= 1 (length (hash-db::sql-tokenize "   ")))) )

(def-test lexer-errors-say-where (:fixture clean-db)
  (flet ((fails (text)
           (handler-case (progn (hash-db::sql-tokenize text) nil)
             (sql-error (c) (list (error-position c) (error-sql c) (error-message c))))))
    (is (equal 7 (first (fails "SELECT 'oops"))))
    (is (search "string is not closed" (third (fails "SELECT 'oops"))))
    (is (equal 7 (first (fails "SELECT \"oops"))))
    (is (equal 2 (first (fails "a /* never closed"))))
    (is (equal 2 (first (fails "a @ b"))))
    (is (search "line 1, column 3" (third (fails "a @ b"))))
    (is (search "line 2, column 1" (third (fails (format nil "a~%@")))) "line and column")
    (is (equal "a @ b" (second (fails "a @ b"))))
    (signals sql-error (hash-db::sql-tokenize "a # b"))
    (signals hash-db-error (hash-db::sql-tokenize "`x`"))))
