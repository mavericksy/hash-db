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

`select` returns a new table. Use `row-list` to get its rows. A query can be written with lambdas, with expressions (plain data), or with both.

```lisp
(row-list (select :from "people"
                  :where '(and (> :age 20) (like :email "%@x"))
                  :col '(:email (as (+ :age 1) :next-age))
                  :order-by '((desc :age) :email)
                  :limit 10 :offset 20))
```

| Keyword | Meaning |
|---------|---------|
| `:from` | table name or a table value |
| `:col` | `t` (all), one item, or a list of items |
| `:where` | function, function name or expression |
| `:group-by`, `:having` | grouping, see below |
| `:distinct` | drop duplicate result rows |
| `:order-by` | sort keys, or a function of two rows |
| `:limit`, `:offset` | cut the result, after sorting |
| `:inner-join`, `:left-join`, `:right-join`, `:full-join`, `:joins` | other tables |

### Lambdas

`:where` takes a function of one row, or the name of one. `matching` builds one from `:column value` pairs. `:order-by` takes a function of two rows that is true when the first row sorts first. A function can also run inside an expression with `(fn f arg ...)`.

```lisp
(select :from "people" :where (lambda (row) (eql 30 (getf row :age))))
(select :from "people" :where (matching "people" :age 30))
(select :from "people" :where '(fn starts-with-a :name))
```

`matching` ignores a column when you give `nil` as its value. `:null` matches NULL.

### Expressions

An expression is data. A keyword is a column. `:null` is NULL. Numbers, strings, `t` and `nil` are constants. `(quote :red)` is a constant keyword.

| Form | Meaning |
|------|---------|
| `(= a b)` `(/= a b)` `(< a b)` `(> a b)` `(<= a b)` `(>= a b)` | compare |
| `(and ...)` `(or ...)` `(not a)` | logic |
| `(in a b c ...)` `(between a lo hi)` | membership and range |
| `(like a "pat%")` | `%` matches any run, `_` one character, `\` escapes |
| `(is-null a)` `(is-not-null a)` `(coalesce a b ...)` | NULL tests |
| `(+ a b ...)` `(- a b)` `(* a b ...)` `(/ a b)` | arithmetic |
| `(lower a)` `(upper a)` `(length a)` `(concat a b ...)` | strings |
| `(fn f a b ...)` | call your function with the values. It sees NULL as `:null` |

Operators match by name, so `(like ...)` and `(:like ...)` both work.

Truth has three values, as in SQL. Comparing with NULL gives unknown. `:where` keeps a row only when the result is true. So `(not (= :age 30))` does not keep a row with a NULL age. An unknown result that is stored or sorted becomes NULL. Arithmetic and string functions return NULL when an argument is NULL.

A wrong column, operator or argument count is an error when the query starts, even for an empty table.

### Items in `:col`

An item is a column, `(as expression :alias)`, or an expression. An unnamed expression is called `:EXPR1`, `:EXPR2` by its place in the list. A list in `:col` is always a list of items, so one expression needs extra brackets: `:col '((+ :age 1))`. Aliases work in `:order-by` and `:having`, not in `:where`.

### Sorting

A key is a column, `(asc expr)` or `(desc expr)`, or any other expression, which sorts up. Add `:nulls :first` or `:nulls :last` to a key. NULL sorts last by default, in both directions. `:order-by` is one key or a list of keys. A list that starts with `asc` or `desc` is one key, so `(desc :age)` needs no extra brackets. For a column named `asc` or `desc`, write `((asc :desc))`. The sort is stable.

### Grouping

```lisp
(select :from "sales"
        :group-by :dept
        :col '(:dept (count) (as (sum :amount) :total))
        :having '(> (sum :amount) 100)
        :order-by '((desc :total)))
```

Aggregates: `(count)`, `(count x)`, `(count-distinct x)`, `(sum x)`, `(avg x)`, `(min x)` and `(max x)`. They skip NULL. `sum`, `avg`, `min` and `max` of no values are NULL. `avg` is a double-float. An unnamed aggregate is called `:COUNT` or `:SUM-AMOUNT`.

- A query is grouped when it has `:group-by` or `:having`, or an aggregate in `:col` or `:order-by`.
- Without `:group-by`, all rows make one group, even when none pass `:where`.
- NULL is a group of its own. Groups come out in the order their first row appeared.
- A grouped query needs `:col`. `:col`, `:having` and `:order-by` can use a `:group-by` expression as written, an alias, and aggregates, even inside other expressions such as `(/ (sum :a) (count))`.
- A column that is not grouped is a `query-error` outside an aggregate. So is an aggregate in `:where`.
- `:group-by` is a column, an expression or a list. A list is always a list of expressions.

### Joins

A join is `(table here there)`. `here` is a column of the rows so far. `there` is a column of the joined table. Rows join where the two values are equal. NULL never matches.

```lisp
(select :from "people"
        :left-join '("pets" :id :owner)
        :group-by :name
        :col '(:name (as (count :pet) :pets)))
```

- `:inner-join` drops rows without a match.
- `:left-join` keeps unmatched rows of the first table, with NULL for the new columns.
- `:right-join` keeps unmatched rows of the joined table, with NULL for the earlier columns, after the matches.
- `:full-join` does both.
- Each keyword takes one join or a list of joins. They apply in that order. `:joins` takes `(kind table here there)` entries, with kind `:inner`, `:left`, `:right` or `:full`, for another order.
- Two tables cannot share a column name. Add `:as prefix` to a join. The columns of that table are then named `:PREFIX.COLUMN`. `there` still uses the plain name. This also lets a table join itself.

### Changing many rows

```lisp
(update-where "people" '(:active nil) :where '(< :age 18))          ; => rows changed
(update-where "people" nil :where t :compute '(:age (+ :age 1)))    ; age = age + 1
(update-where "people" (lambda (row) (list :name (string-upcase (getf row :name))))
              :where t)
(delete-where "people" :where '(is-null :email))                     ; => rows deleted
```

- The changes are `:column value` pairs, or a function of the old row that returns them. `:compute` takes `:column expression` pairs, worked out on the old row.
- Both run in one transaction. All rows change or none do.
- `:where` is required. Use `:where t` for every row.
- `delete-where` applies `:on-delete` rules to each row. Its count does not include rows that a cascade deleted.

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
