(asdf:defsystem #:hash-db
  :name "hash-db"
  :version "1.0.0"
  :license "MIT"
  :author "Shaun Pearce <mavericksy@gmail.com>"
  :maintainer "Shaun Pearce <mavericksy@gmail.com>"
  :description "Simple hash-table DB"
  :depends-on (#:cl-store
               #:fset
               (:feature :sbcl (:require #:sb-posix))
               #:iterate
               #:alexandria
               #:make-hash
               #:bordeaux-threads)
  :serial t
  :components ((:file "package")
               (:file "conditions")
               (:file "hash-db")
               ;; the write-ahead log needs SBCL: sb-posix and sb-ext
               (:file "wal" :if-feature :sbcl))
  :in-order-to ((test-op (test-op #:hash-db/tests))))

(asdf:defsystem #:hash-db/tests
  :description "FiveAM tests for hash-db"
  :depends-on (#:hash-db #:fiveam)
  :serial t
  :components ((:module "tests"
                :components ((:file "package")
                             (:file "tests"))))
  :perform (test-op (o c)
             (let ((results (uiop:symbol-call :fiveam '#:run
                                              (uiop:find-symbol* '#:hash-db-suite :hash-db-tests))))
               (uiop:symbol-call :fiveam '#:explain! results)
               (unless (uiop:symbol-call :fiveam '#:results-status results)
                 (error "hash-db tests failed")))))
