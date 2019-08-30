(ql:quickload :hash-db)
    
(in-package :hash-db)
(use-package :alexandria)
(defvar h  (create-hash-table))
(!null '())

(defparameter *basic-schema* (make-schema '((a string) (b number) (c number))))

(type-of (car *basic-schema*))

(defparameter *basic-db-table-ws* 
  (make-table "a-table" (make-schema '((a-number number)
                                      (a-string string)))))

(defparameter *basic-db-table* 
  (make-table "b-table" '((a-number number)
                          (a-string string))))

*basic-db-table*
*basic-db-table-ws*

(unless (eql (type-of *basic-db-table-ws*) 'table) (format t "~a" "HI"))
 
*global-tables*
(gethash "a-table" *global-tables*)
 
(show-tables)

(defvar *sel*  (select :from "a-table"))

(hash-table-keys (rows *sel*))
(hash-table-values (rows *sel*))
(getf  (hash-table-plist (rows *sel*)) 4)

(defvar *ins* (list :a-number 1 :a-string "string"))
(defvar *ins2* (list :a-number 2 :a-string "str"))

(getf *ins2* :a-number)

(insert-row *ins2* "a-table")
(insert-row *ins* "no-table")
