# HASH-DB

A small in-memory database for Common Lisp. Tables hold rows in hash tables. You can save all tables to a file and load them again.

## Requirements

- SBCL and Quicklisp.
- Quicklisp systems: `cl-store`, `iterate`, `alexandria`, `make-hash`, `bordeaux-threads`.

## Load

Put this directory in `~/quicklisp/local-projects/` or add it to `asdf:*central-registry*`. Then run:

```lisp
(ql:quickload :hash-db)
(in-package :hash-db)
```

## Use

Column names are keywords. A symbol or string works too. Column types are `string`, `number`, `boolean`, `interned-string`, `primary-key-column` and `foreign-key-column`.

```lisp
(make-table "people" '((:id primary-key-column)
                       (:name string)
                       (:age number)))
;; a foreign key column is (name type default referenced-table)
(make-table "pets" '((:pet-id primary-key-column)
                     (:owner foreign-key-column nil "people")
                     (:pet string)))

(insert-row '(:name "ann" :age 30) "people")   ; => 1, the row id
(insert-row '(:owner 1 :pet "rex") "pets")     ; => 1
(insert-row '(:owner 9 :pet "x") "pets")       ; error: foreign key violation

(get-row 1 "people")                           ; => (:ID 1 :NAME "ann" :AGE 30)
(update-row 1 '(:age 31) "people")
(delete-row 1 "people")

;; select returns a new table. Use row-list to get its rows.
(row-list (select :from "people"
                  :where (matching "people" :age 30)
                  :col '(:name :age)
                  :order-by :age
                  :distinct t))

(row-list (select :from "pets"
                  :inner-join '("people" :owner :id)
                  :col '(:pet :name)))

(save-db "my.db")
(load-db "my.db")                              ; replaces all tables
```

Rules:

- The primary key column holds the row id. The database generates it. You cannot set it.
- A foreign key value must be the id of a row in the referenced table.
- A join fails if both tables have a column with the same name.
- `matching` ignores a column when you give `nil` as its value.
- `delete-row` does not check if other rows refer to the deleted row.

## Test

```
sbcl --noinform --eval '(push (truename ".") asdf:*central-registry*)' \
     --eval '(asdf:test-system :hash-db)' --eval '(quit)'
```
