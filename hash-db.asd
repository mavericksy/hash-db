(asdf:defsystem #:hash-db
  :name "hash-db"
  :version "1.0.0"
  :license "MIT"
  :author "Shaun Pearce <mavericksy@gmail.com>"
  :maintainer "Shaun Pearce <mavericksy@gmail.com>"
  :description "Simple hash-table DB"
  :depends-on (#:cl-store
               #:iterate
               #:alexandria
               #:make-hash
               #:bordeaux-threads)
  :serial t
  :components ((:file "package")
               (:file "hash-db")))
