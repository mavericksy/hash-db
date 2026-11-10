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
| `json` | JSON text. See JSON |
| `primary-key-column` | generated row ids. You cannot set them |
| `foreign-key-column` | the id of a row in another table |

| Option | Meaning |
|--------|---------|
| `:default value` | value for a new row that does not give one |
| `:default-fn fn` | function called for each new row that does not give a value |
| `:not-null t` | reject NULL |
| `:unique t` | reject a second row with the same non-null value |
| `:check c` | reject a value when the check is false. `c` is a function, a function name, or an expression about this column such as `(and (>= :age 0) (< :age 150))`. It does not run on NULL |
| `:references "table"` | foreign key only. The table that holds the referenced rows |
| `:on-delete action` | foreign key only. `:restrict` (default), `:cascade` or `:set-null` |

`:default-fn` and `:check` take a function or a function name. `:check` also takes an expression. `save-db` can only save a function name or an expression. It signals `persistence-error` for a function object such as a `lambda`.

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
| `(case c1 v1 c2 v2 ... [else])` | the value of the first true condition. Only that value is worked out. No else gives NULL |
| `(fn f a b ...)` | call your function with the values. It sees NULL as `:null` |
| `(date "2024-01-31")` `(timestamp "2024-01-31 12:30:00")` `(today)` | a universal time. See Dates |
| `(date-text x)` `(timestamp-text x)` `(year x)` `(month x)` `(day x)` `(hour x)` `(minute x)` `(second x)` `(dow x)` | read a universal time |

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

## Indexes

An index finds rows by the value of one column without a scan.

```lisp
(create-index "people" :age)                     ; => :AGE-INDEX
(create-index "people" :email :unique t :name :email-key)
(show-indexes "people")
(drop-index :age-index "people")                 ; => t, or nil if there is none
```

- A `:unique` column and a `foreign-key-column` get an index by themselves. It is part of the schema. You cannot drop it. The primary key needs none, because rows are stored by id.
- `create-index` takes one column. `:unique t` also enforces uniqueness, and it fails without changing anything if the rows already hold a duplicate. NULL is not indexed, so NULL values never clash.
- `create-index` and `drop-index` are transactions. The log and the snapshot keep the indexes that `create-index` made. An older version 3 file loads without them.
- Indexes belong to the same immutable snapshot as the rows. A reader never sees an index that does not match its rows.
- The checks for `:unique` and the search for child rows in a cascade use these indexes, so they do not scan the table.

### What a query uses

`select`, `update-where` and `delete-where` read through an index when the `:where` is an expression. The planner looks at the parts of an `and` that compare one column of the first table with constants:

`(= :c v)` `(< :c v)` `(> :c v)` `(<= :c v)` `(>= :c v)` `(between :c lo hi)` `(in :c v ...)`

- A constant can be an expression without columns, such as `(+ 5 5)`. A constant on the left works too: `(< 10 :c)`.
- Two limits on one column make one range: `(and (> :c 1) (< :c 9))`.
- The planner picks the condition that should find the fewest rows. The primary key is best, then a unique index, an equality, an `in`, and then a range.
- The whole `:where` still runs on the rows it finds. A plan can change the speed. It never changes the result, and rows still come out in id order.
- A range works on columns of numbers or of strings. Strings compare by character code.
- These make a scan: a function in `:where`, `or`, `not`, a column without an index, a range on another type of column, and a condition on a column of a joined table. A right or full join reads the whole first table.

### Joins

A join uses an index on the joined column when the join is inner or left and fewer than a quarter as many rows look as the joined table holds. Otherwise it builds one hash table for the query. The rows come out in the same order as before.

### Explain

`explain` takes the arguments of `select` and returns the steps without running the query.

```lisp
(explain :from "people" :where '(and (= :city "oslo") (> :age 20)) :order-by :age :limit 5)
;; => ((:STEP :SCAN :TABLE "people" :ACCESS :INDEX :INDEX :CITY-INDEX :COLUMN :CITY
;;            :OP :EQ :ROWS 12)
;;     (:STEP :FILTER :WHERE (AND (= :CITY "oslo") (> :AGE 20)))
;;     (:STEP :SORT :KEYS ((:KEY :AGE :DIRECTION :ASC :NULLS :LAST)))
;;     (:STEP :LIMIT :LIMIT 5 :OFFSET NIL))
```

The scan step reads the index it would use, so its row count is exact. When there is no index step, it says why with `:reason`: `:function`, `:right-or-full-join` or `:no-index-condition`. A join step shows `:method` as `:index`, `:hash` or `:index-or-hash`.

`scripts/bench.lisp` times queries with and without an index on your machine.

## Sequences

A sequence hands out numbers one after another. It lives next to the tables and follows transactions: a number taken in a transaction that rolls back is given again.

```lisp
(create-sequence "order-numbers" :start 1000)   ; :increment can be negative
(next-value "order-numbers")                    ; => 1000
(current-value "order-numbers")                 ; the last number given, or nil
(restart-sequence "order-numbers" 5000)
(drop-sequence "order-numbers")
(show-sequences)
```

A sequence can fill a column when you give a named function:

```lisp
(defun next-order-number () (next-value "order-numbers"))
(make-table "orders" '((:id primary-key-column) (:no integer :default-fn next-order-number)))
```

Tables, views and sequences share one namespace. `show-tables` lists tables only.

## Views

A view is a stored query. It holds no rows. Each time you query it, the query runs on the data of that moment.

```lisp
(create-view "adults" :from "people" :where '(>= :age 18) :col '(:id :name) :order-by :id)
(select :from "adults" :where '(like :name "a%"))     ; a view works where a table name does
(drop-view "adults")
(show-views)
```

- The arguments after the name are the arguments of `select`. The query runs once when you make the view, so a wrong table or column is an error then.
- A view works in `:from`, in a join, in another view, in `explain`, in `matching` and in the export functions.
- A view cannot be changed. `insert-row`, `update-where`, `create-index` and the like signal `schema-error`.
- A view does not follow a table that is renamed or dropped. The query then fails when it runs.

## Triggers

A trigger is a function that runs when a row of a table is inserted, updated or deleted.

```lisp
(defun audit (event table old new)
  (insert-row (list :what (string-downcase (string event)) :tbl table) "audit"))
(create-trigger :audit "people" :after '(:insert :update :delete) 'audit)
(show-triggers "people")
(drop-trigger :audit "people")
```

The function takes the event (`:insert`, `:update` or `:delete`), the table name, the old row (nil for an insert) and the new row (nil for a delete).

- A `:before` trigger runs before the change. It can return `:column value` pairs that change the new row, and the checks of the schema run on them. It can signal an error to stop the change. It may change other tables, but changing its own table is a `trigger-error`.
- An `:after` trigger sees the new state and may do anything.
- A trigger runs inside the transaction of the change. An error drops the whole change, and what a trigger wrote rolls back with it.
- Cascades, `update-where`, `delete-where`, imports and purges fire triggers for each row. Replaying the log does not, because what the triggers did was logged with the change.
- Triggers nest at most 32 levels, so a loop is a `trigger-error`.
- Give a function name, not a lambda, if the trigger must survive a save or a restart.

## Expiry

```lisp
(make-table "sessions" '((:id primary-key-column) (:user string) (:expires timestamp)))
(set-ttl "sessions" :expires)                          ; makes an index on the column
(insert-row (list :user "ann" :expires (expires-in 3600)) "sessions")
(purge-expired)                                        ; => rows deleted. :table limits it to one table
(start-sweeper :interval 60)                           ; purge in the background
(stop-sweeper)
```

- A row whose timestamp is at or before now has expired. NULL never expires.
- Expired rows stay in the table, and queries see them, until a purge deletes them. To hide them at once, add `(> :expires (fn get-universal-time))` to `:where`.
- A purge is a `delete-where`: it reads only the expired rows through the index, runs cascades and triggers, is logged, and is all or nothing.
- `*clock*` and `(now)` give the time. Set `*clock*` to test expiry. `clear-ttl` turns expiry off.
- `start-sweeper` takes `:on-error`, a function that gets the condition when a purge fails. The sweeper goes on.

## Changing tables and migrations

```lisp
(add-column "people" '(:email string :default "none"))
(rename-column "people" :email :mail)
(drop-column "people" :mail)
(rename-table "people" "humans")
```

- The rows, indexes, checks, triggers and ttl follow a rename. A dropped column takes its indexes and ttl with it. The primary key and the only column of a table cannot be dropped.
- Renaming a table also points the foreign keys of other tables, and its own self reference, at the new name.
- A view or a trigger function that names the old column or table is not changed.

`migrate` runs a function once and remembers it in the table `_migrations`.

```lisp
(migrate-all (list (list "001-people" (lambda () (make-table "people" ...)))
                   (list "002-email"  (lambda () (add-column "people" '(:email string))))))
;; => the ids that ran now. Call it each time the program starts.
(applied-migrations)
```

A migration and its note are one transaction. If the function fails, nothing it did stays, and the migration is not noted.

## JSON

A `json` column holds JSON text. It is checked when a row is written and kept in compact form, so equal documents are equal text.

```lisp
(make-table "docs" '((:id primary-key-column) (:body json)))
(insert-row (list :body "{\"name\": \"ann\", \"tags\": [\"a\", \"b\"]}") "docs")
(select :from "docs" :where '(= (json-get :body "name") "ann"))
(select :from "docs" :col '((as (json-get :body "tags" 0) :first-tag)))
```

`(json-get doc step ...)` goes down the path. A step is a string for an object key or a whole number for an array place that counts from 0. It gives a string, a number, `t`, `nil` or `:null`. For an object or array it gives the JSON text. A missing step gives `:null`.

`json-decode` and `json-encode` read and write JSON in Lisp data: an object is `(:object ("key" . value) ...)`, an array is a vector, and `:true`, `:false` and `:null` are the constants.

## Import and export

```lisp
(export-csv "people" "people.csv")       ; a path or a stream. :columns picks columns
(import-csv "people" "people.csv")       ; => rows added
(export-json "people" "people.json")
(import-json "people" "people.json")
```

- The table can be a table name, a view, or a table value from `select`.
- An import is one transaction: every row goes in or none does. Checks, triggers and the log apply to each row. Errors name the line or row (`import-error`).
- CSV: an empty field is NULL and `""` is an empty string. The first line names the columns. `:columns` names them instead, and `:header nil` says there is no header line. Without both, the fields are all columns but the primary key. An empty field gives NULL, not the column default.
- JSON: the file is an array of objects. Keys can be in any case. A json column is written as JSON, not as a string. A fraction such as 1/2 is written as a decimal.
- Ids are generated, so values for a primary key column in a file are ignored.
- A file path is written through a temporary file and renamed.

## Dates

A `timestamp` column holds a universal time: whole seconds since 1900-01-01 00:00:00 UTC. The date expressions work on it, in UTC, from 1900 on.

```lisp
(select :from "events" :where '(between :at (date "2024-02-01") (timestamp "2024-02-29 23:59:59")))
(select :from "events" :col '((as (date-text :at) :day) (as (year :at) :y)) :where '(= (dow :at) 1))
(parse-timestamp "2024-01-31 12:30:00")      ; => a universal time
(format-timestamp 3919200000)                ; => "2024-01-31 12:00:00"
```

- `(date text)` and `(timestamp text)` read `YYYY-MM-DD`, or `YYYY-MM-DD HH:MM[:SS]` with a space or `T` and an optional `Z`. A bad date, such as February 30, is a `query-error`.
- `(today)` is the start of today. It uses `(now)`, so setting `*clock*` sets it.
- `(dow x)` is 0 for Sunday to 6 for Saturday.
- To add time, add seconds: `(+ :at 86400)` is a day later. NULL gives NULL.
- A date given as a constant is worked out first, so an index on the column is used.

## History

```lisp
(keep-history 100)                                   ; keep the last 100 versions in memory
(history)                                            ; => ((:seq 41 :time 3900000000) ...)
(with-as-of (:seq 41) (row-list "people"))
(with-as-of (:time (- (now) 3600)) (select :from "people"))
```

Every commit that changes data makes a new version. An old version stays whole, because the new one shares what did not change. Reading it needs no copy and no replay. Inside `with-as-of` reads see the old data and any change signals `transaction-error`. For `:time`, the body sees the newest version made at or before that time.

History is off by default and kept in memory only. Kept versions hold the memory of rows that later changes replaced. It starts again after a restart.

## SQL

`sql` runs one statement of SQL. It reads the text, looks up the names in your tables, and calls `select`, `update-where`, `create-index` and the rest. So everything above holds for it: transactions, indexes, triggers, the log.

```lisp
(sql "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, city TEXT)")
(sql "INSERT INTO customers (name, city) VALUES (?, ?)" "ann" "oslo")   ; => 1, 1  (rows, last id)
(sql-rows "SELECT name FROM customers WHERE city = ? ORDER BY name LIMIT 10" "oslo")
;; => ((:NAME "ann"))
(sql "UPDATE customers SET city = upper(city) WHERE name = 'ann'")      ; => rows changed
(sql "DELETE FROM customers WHERE city IS NULL")                         ; => rows deleted
(sql "EXPLAIN SELECT * FROM customers WHERE name = 'ann'")               ; the steps, as explain gives them
```

- `sql` gives a table for `SELECT`. `sql-rows` gives the rows as plists. `INSERT` gives the row count and the last id. `UPDATE` and `DELETE` give the row count.
- A `?` takes the next value after the text. A value is data and never SQL, so there is no injection. Pass `:null` for NULL. A wrong number of values is an error.
- `sql-script` runs several statements, separated by semicolons, as one transaction. The `?` of all of them take the values in order. It gives a list of the results. A script can make a table and then fill it.
- `(sql-expression "age >= ? AND name LIKE 'a%'" "people" 18)` gives an expression for `:where` and the like, so you can mix SQL and the other forms.

**What it reads**

| Statement | Notes |
|-----------|-------|
| `SELECT` | `DISTINCT`, `*` and `t.*`, `expr AS alias`, `FROM table [alias]`, `FROM (SELECT ...) alias`, `[INNER\|LEFT\|RIGHT\|FULL [OUTER]] JOIN t [alias] ON a = b` in a chain, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY ... [ASC\|DESC] [NULLS FIRST\|LAST]`, `LIMIT`, `OFFSET` |
| `UNION [ALL]`, `INTERSECT`, `EXCEPT` | between `SELECT`s, in brackets if you like, with `ORDER BY`, `LIMIT` and `OFFSET` for the whole result |
| `INSERT` | `INSERT INTO t [(cols)] VALUES (...), (...)` and `INSERT INTO t [(cols)] SELECT ...`. `DEFAULT` leaves a column out |
| `UPDATE`, `DELETE` | with or without `WHERE`. Without it they change every row |
| `CREATE TABLE` | types `INTEGER`, `REAL`/`FLOAT`, `NUMERIC`, `TEXT`/`VARCHAR`, `BOOLEAN`, `TIMESTAMP`, `JSON`. `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `DEFAULT`, `CHECK`, `REFERENCES t(id) [ON DELETE CASCADE\|SET NULL\|RESTRICT]`, as column or table constraints. `IF NOT EXISTS` |
| `CREATE INDEX [UNIQUE]`, `CREATE VIEW`, `CREATE SEQUENCE` | `CREATE INDEX [name] ON t (col)`, `CREATE VIEW v AS SELECT ...`, `CREATE SEQUENCE s [START [WITH] n] [INCREMENT [BY] n]` |
| `DROP TABLE`, `VIEW`, `INDEX`, `SEQUENCE` | with `IF EXISTS`. `DROP INDEX name [ON table]` |
| `ALTER TABLE` | `ADD [COLUMN] def`, `DROP [COLUMN] c`, `RENAME [COLUMN] a TO b`, `RENAME TO t` |
| `EXPLAIN SELECT` | |

Expressions: `OR AND NOT`, `= <> != < > <= >=`, `IS [NOT] NULL`, `[NOT] IN (...)`, `[NOT] IN (SELECT ...)`, `EXISTS (SELECT ...)`, `(SELECT ...)` as a value, `[NOT] BETWEEN`, `[NOT] LIKE`, `CASE WHEN ... THEN ... [ELSE ...] END` and `CASE x WHEN ...`, `+ - * /`, `||`, `DATE '2024-01-31'`, `TIMESTAMP '2024-01-31 12:30:00'`, `CURRENT_DATE`, `EXTRACT(YEAR FROM x)`, and the functions `LOWER`, `UPPER`, `LENGTH`, `COALESCE`, `COUNT` (also `COUNT(*)` and `COUNT(DISTINCT x)`), `SUM`, `AVG`, `MIN`, `MAX`, `JSON_GET`, `NOW()`, `CURRENT_TIMESTAMP`, `DATE_TEXT`, `TIMESTAMP_TEXT`, `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DOW`, `NEXTVAL('s')` and `CURRVAL('s')`.

**Subqueries** are worked out once, when the statement starts, and the answer becomes a constant. So a subquery cannot use the tables of the query around it. A correlated subquery is refused, and the message says which name belongs to the outer query. A subquery that is a value must give one column and at most one row (none gives NULL). `x IN (SELECT ...)` over an empty answer is false. A view cannot hold a subquery, because it would keep the answer and not the query. An index of the outer table is used for the constant, as for any other. `EXPLAIN` works the subqueries out too.

**Set operations** match columns by position, name them by the first query, and need the same number of columns. Rows are equal when all their values are equal, NULL equals NULL, and `1` equals `1.0`. `INTERSECT` binds tighter than `UNION` and `EXCEPT`. `INTERSECT ALL` and `EXCEPT ALL` are refused. `EXPLAIN` and `CREATE VIEW` take one `SELECT` only.

**Names and types**

- Names are not case sensitive. A table name that is not `"quoted"` is read in lower case. It also finds a table that has capitals if no other table matches. Quote a name to match it exactly.
- A column name in SQL has no hyphen, so `pet_ref` also finds the column `PET-REF`. An exact match comes first.
- Each joined table is joined with its alias, or its name, as the prefix. `SELECT *` and `t.*` give plain column names. When two items would have the same name, they are named `table.column`.
- `id INTEGER PRIMARY KEY` is a generated key. Leave it out of `INSERT`.
- `TIMESTAMP`, `DATETIME` and `DATE` are one type. They hold a universal time, a whole number of seconds since 1900-01-01 UTC, not a date text. Write dates as `DATE '2024-01-31'`. `TEXT` is a string.
- A `CHECK` is kept as an expression, so it survives a save and a restart. It can name one column.

**Not supported.** Each gives a `sql-error` that says so: correlated subqueries, `INTERSECT ALL` and `EXCEPT ALL`, `SELECT` without `FROM`, a list of tables (`FROM a, b`), `CROSS` and `NATURAL JOIN`, `USING`, an `ON` that is not one equality, `ORDER BY` a column number, `%`, `INTERVAL`, keys and unique constraints of several columns, named constraints, `ON UPDATE`, `ADD CONSTRAINT`, and types such as `BLOB`.

A `sql-error` tells the line and column, and shows the text near it. `error-position` and `error-sql` give the details.

## Errors

Every error that hash-db signals is a `hash-db-error`.

```
hash-db-error
  no-such-table, no-such-column, no-such-row, no-such-sequence
  schema-error
    table-exists
  invalid-row
  query-error
  file-format-error
  sql-error
  persistence-error
  transaction-error
  trigger-error
  history-error
  json-error
  import-error
  constraint-violation
    not-null-violation, type-mismatch, unique-violation,
    check-violation, foreign-key-violation
```

Use `error-table`, `error-column`, `error-id`, `error-file`, `error-name`, `error-line` and `error-message` to read the details.

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
- Sequences, views, triggers, ttl and indexes are saved and logged like tables. A view or trigger that holds a lambda works in memory. A save or a logged commit refuses it with `persistence-error`. Use function names.
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
