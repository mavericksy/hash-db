# HASH-DB

A small in-memory database for Common Lisp. Tables hold rows in hash tables. You can save all tables to a file and load them again.

## Requirements

- SBCL and Quicklisp.
- Quicklisp systems: `cl-store`, `iterate`, `alexandria`, `make-hash`, `bordeaux-threads`, `fset`.

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

## Transactions

`with-transaction` runs its body as one change. All of it happens, or none of it.

```lisp
(with-transaction
  (let ((owner (insert-row '(:email "bob@x" :age 40) "people")))
    (insert-row (list :owner owner :pet "tom") "pets")))
```

- The changes become visible when the body returns. An error or any other non-local exit drops them.
- Inside the body, reads see the changes made so far.
- Other threads see only committed data. They never wait for a reader.
- Writers wait for each other. A read-modify-write inside a transaction loses no updates.
- Each of `insert-row`, `update-row`, `delete-row`, `add-column`, `make-table` and `drop-table` is a transaction by itself.
- A `with-transaction` inside another one is a savepoint. If it fails, only its own changes are dropped.
- `(rollback)` leaves the innermost `with-transaction` and drops its changes. That `with-transaction` returns `nil`.
- Dropped inserts give their row ids back.
- `load-db` signals `transaction-error` inside a transaction.

## Snapshots

`with-snapshot` fixes the committed state that a thread reads. Every read in its body sees the same data, even if other threads commit meanwhile.

```lisp
(with-snapshot
  (list (row-list "people") (row-list "pets")))   ; both from the same state
```

`select` runs in a snapshot, so a join reads both tables from the same state. Inside a transaction, `with-snapshot` does nothing.

A table value is immutable. A table that `find-table` or `select` returns does not change later. Call `find-table` again to see new data. The write functions take a table name.

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
  persistence-error
  transaction-error
  constraint-violation
    not-null-violation, type-mismatch, unique-violation,
    check-violation, foreign-key-violation
```

Use `error-table`, `error-column`, `error-id`, `error-file` and `error-message` to read the details.

## Durability

`open-db` makes the database durable. Every committed transaction goes to a log file and is flushed to disk before it becomes visible. After a crash, the next `open-db` replays the log.

```lisp
(open-db "my.db")      ; files: my.db (snapshot) and my.db.wal (log)
(make-table ...)       ; from here on, every commit is logged
(checkpoint)           ; fold the log into the snapshot now
(close-db)             ; checkpoint, then stop logging
```

- `open-db` creates the files if they do not exist. If tables are in memory and no files exist, the tables become the first snapshot. If files exist, memory must be empty.
- A commit is durable when the function that made it returns. If the log write fails, the transaction does not commit and you get `persistence-error`.
- The log folds into the snapshot when it passes `:checkpoint-bytes` (default 8 MiB, `nil` turns it off). A failed automatic checkpoint only warns. The commit is already in the log.
- `:sync :always` (default) flushes every commit. `:sync :never` leaves flushing to the operating system. A killed process still keeps its commits. A power loss can lose the last ones.
- `(close-db :checkpoint nil)` keeps the log as it is.
- After a crash, the log can end in a partial record. `open-db` cuts it off and signals the warning `wal-truncated`. Everything before the damage is kept. If a record in the middle is damaged, the records after it are lost too.
- A jump in transaction numbers in the log signals `persistence-error`, because data is missing.
- Values must print and read back. Strings, numbers, keywords, symbols, lists and vectors do. A function, a stream or a hash table does not, and the commit fails with `persistence-error`.
- `save-db` and `load-db` stay for plain snapshots. `save-db` refuses the snapshot of the open database. `load-db` refuses an open database.

Limits:

- The log needs SBCL. `save-db` and `load-db` work on other implementations, but without flushing.
- One process may open a database at a time. There is no lock file.
- On macOS, `fsync` does not force the drive to empty its own cache. A power loss can still lose recent commits there.

## Save and load

```lisp
(save-db "my.db")   ; atomic: write a temporary file, flush, rename
(load-db "my.db")   ; replaces all tables
```

`save-db` writes the last committed state. A crash or an error leaves the old file whole.

## Threads

One writer runs at a time. Readers take no lock. A change builds new immutable tables, then swaps them in as one step. A reader never sees a change half done, including a cascade.

`save-db` writes the last committed state. It does not save the open changes of a transaction. Writers wait only while it reads the root.

## Test

```
scripts/run-tests.sh
```
