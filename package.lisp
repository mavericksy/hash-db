(in-package #:cl-user)

(defpackage #:hash-db-system
  (:use #:cl #:asdf))

(in-package #:hash-db-system)

(defpackage #:hash-db
  (:use
    #:cl
    #:iterate
    #:bordeaux-threads
    #:cl-store
    #:alexandria)
  (:export #:make-class-specific-col
           #:show-tables
           #:insert-row
           #:make-schema
           #:make-table
           #:select
           #:get-named-database
           #:get-schema))
