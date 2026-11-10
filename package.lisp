(defpackage #:hash-db
  (:use
    #:cl
    #:iterate
    #:bordeaux-threads
    #:cl-store
    #:alexandria)
  (:export
    ;; tables
    #:table #:make-table #:drop-table #:show-tables #:get-named-table #:find-table
    #:name #:rows #:schema #:get-schema #:add-column
    ;; schema and columns
    #:make-schema #:make-col #:make-class-specific-col
    #:primary-key-column #:foreign-key-column #:interned-string
    #:timestamp #:any
    ;; transactions
    #:with-transaction #:rollback #:with-snapshot
    ;; sql
    #:sql #:sql-rows #:sql-script #:sql-expression
    #:sql-error #:error-position #:error-sql
    ;; history
    #:keep-history #:history #:with-as-of #:history-error
    ;; import and export
    #:export-csv #:import-csv #:export-json #:import-json #:import-error #:error-line
    ;; json
    #:json #:json-encode #:json-decode #:json-error
    ;; changing tables and migrations
    #:rename-table #:rename-column #:drop-column #:migrate #:migrate-all
    #:applied-migrations
    ;; expiry
    #:set-ttl #:clear-ttl #:ttl-column #:purge-expired #:expires-in #:now #:*clock*
    #:start-sweeper #:stop-sweeper
    ;; triggers
    #:create-trigger #:drop-trigger #:show-triggers
    ;; views
    #:create-view #:drop-view #:show-views
    ;; sequences
    #:create-sequence #:drop-sequence #:next-value #:current-value #:restart-sequence
    #:show-sequences
    ;; rows
    #:insert-row #:get-row #:update-row #:delete-row #:row-list #:update-where #:delete-where
    ;; indexes
    #:create-index #:drop-index #:show-indexes
    ;; queries
    #:select #:matching #:explain
    ;; persistence
    #:save-db #:load-db #:open-db #:close-db #:checkpoint
    ;; conditions
    #:hash-db-error #:error-message #:error-table #:error-column #:error-id #:error-file
    #:no-such-table #:no-such-column #:no-such-row #:no-such-sequence #:error-name
    #:schema-error #:table-exists #:invalid-row #:query-error #:transaction-error #:trigger-error
    #:constraint-violation #:not-null-violation #:type-mismatch #:unique-violation #:check-violation
    #:foreign-key-violation
    #:file-format-error #:persistence-error #:wal-truncated #:truncated-bytes))
