(in-package #:cl-user)

(defpackage #:hash-db-system
  (:use #:cl #:asdf))

(in-package #:hash-db-system)

(defpackage #:hash-db
  (:use
    #:cl
    #:bordeaux-threads
    #:cl-store
    #:alexandria)
  (:export #:get-inc-unq-id
           #:get-by-key
           #:get-lock
           #:get-database
           #:ensure-gethash-local
           #:*write-lock*
           #:*read-lock*
           #:*id-lock*))
