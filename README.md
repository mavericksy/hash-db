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

## Tables and columns

A column is `(name type :option value ...)`. Column names are keywords. A symbol or string works too.

| Type | Accepts |
|------|---------|
| `string`, `interned-string` | strings. `interned-string` keeps one copy of equal strings and is never NULL |
| `number` | any real number |
| `integer` | integers |
| `float` | any real number, stored as `double-float` |
| `boolean` | `t` or `nil` |
| `timestamp` | a universal time (integer, 0 or more) |
| `keyword` | keywords |
| `any` | any Lisp object. You cannot sort by it |
| `primary-key-column` | generated row ids. You cannot set them |
| `foreign-key-column` | the id of a row in another table |

| Option | Meaning |
|--------|---------|
| `:default value` | value for a new row that does not give one |
| `:default-fn fn` | function called for each new row that does not give a value |
| `:not-null t` | reject NULL |
| `:unique t` | reject a second row with the same non-null value |
| `:check fn` | reject a value when `fn` returns nil. `fn` does not run on NULL |
| `:references "table"` | foreign key only. The table that holds the referenced rows |
| `:on-delete action` | foreign key only. `:restrict` (default), `:cascade` or `:set-null` |

`:default-fn` and `:check` take a function or a function name. `save-db` can only save a function name. It signals `schema-error` for a function object such as a `lambda`.

```lisp
(make-table "people" '((:id primary-key-column)
                       (:email string :unique t :not-null t)
                       (:age integer :check adult-p)
                       (:created timestamp :default-fn get-universal-time)))

(make-table "pets" '((:pet-id primary-key-column)
                     (:owner foreign-key-column :references "people" :on-delete :cascade)
                     (:pet string)))
```

## Rows

```lisp
(insert-row '(:email "ann@x" :age 30) "people")   ; => 1, the row id
(get-row 1 "people")
(update-row 1 '(:age 31) "people")
(delete-row 1 "people")                           ; => t, or nil if there is no such row
```

A primary key column holds the row id.

## NULL

A column with no value and no default holds `:null`. NULL is not the same as false (`nil`).

- NULL passes every type check unless the column is `:not-null`.
- `UNIQUE` allows many NULL values.
- NULL sorts after all other values in `:order-by`.
- `:distinct` treats two NULL values as equal.
- A join never matches NULL.
- `(matching "people" :age :null)` finds the rows where `age` is NULL.

## Foreign keys

A foreign key value must be the id of a row in the referenced table. It can be NULL unless the column is `:not-null`.

When you delete a row that other rows refer to, the `:on-delete` action of those columns runs:

- `:restrict` signals `foreign-key-violation`.
- `:cascade` deletes the rows that refer to it.
- `:set-null` sets the foreign key to NULL.

`delete-row` plans the whole cascade first. If any step is blocked, it changes nothing. `drop-table` signals `schema-error` while another table refers to the table.

## Queries

`select` returns a new table. Use `row-list` to get its rows.

```lisp
(row-list (select :from "people"
                  :where (matching "people" :age 30)
                  :col '(:email :age)
                  :order-by :age
                  :distinct t))

(row-list (select :from "pets"
                  :inner-join '("people" :owner :id)
                  :col '(:pet :email)))
```

- `:where` takes a function of one row. `matching` builds one from `:column value` pairs.
- `matching` ignores a column when you give `nil` as its value.
- A join fails if both tables have a column with the same name.

## Errors

Every error that hash-db signals is a `hash-db-error`.

```
hash-db-error
  no-such-table, no-such-column, no-such-row
  schema-error
    table-exists
  invalid-row
  query-error
  file-format-error
  constraint-violation
    not-null-violation, type-mismatch, unique-violation,
    check-violation, foreign-key-violation
```

Use `error-table`, `error-column`, `error-id`, `error-file` and `error-message` to read the details.

## Save and load

```lisp
(save-db "my.db")
(load-db "my.db")   ; replaces all tables
```

## Threads

All changes take one database-wide writer lock. Readers take a table lock. A reader can see a cascade half done. Snapshot reads are planned for a later version.

## Test

```
scripts/run-tests.sh
```
