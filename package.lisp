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
    ;; rows
    #:insert-row #:get-row #:update-row #:delete-row #:row-list
    ;; queries
    #:select #:matching
    ;; persistence
    #:save-db #:load-db
    ;; conditions
    #:hash-db-error #:error-message #:error-table #:error-column #:error-id #:error-file
    #:no-such-table #:no-such-column #:no-such-row
    #:schema-error #:table-exists #:invalid-row #:query-error
    #:constraint-violation #:not-null-violation #:foreign-key-violation
    #:file-format-error))
