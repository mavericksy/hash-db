;;;; Views: stored queries.
;;;;
;;;; A view has a name and the arguments of a select. It holds no rows. Each time
;;;; you query it, the select runs on the data of that moment, and the result is a
;;;; table value. So a view works wherever a table name does for reading: in
;;;; :from, in a join, in another view, in explain, in matching. It cannot be
;;;; changed. A view does not follow a table that is renamed or dropped, and the query
;;;; fails when it runs.

(in-package :hash-db)

;; Make a view. QUERY is the arguments of select: (create-view "v" :from "t" :where ...).
;; The query runs once now, so a wrong table or column is an error here.
(defun create-view (name &rest query)
  (unless (getf query :from)
    (error 'schema-error :message "A view needs :from"))
  (with-transaction
    (check-name-free name "view")
    (apply #'select query)
    (let ((query (copy-tree query)))
      (put-object name (make-view name query))
      (journal-add (list :create-view name query))
      name)))

;; Drop a view. Return t, or nil if there is none.
(defun drop-view (name)
  (with-transaction
    (when (typep (named-object name) 'view)
      (remove-object name)
      (journal-add (list :drop-view name))
      t)))

;; All views as (:name n :query q).
(defun show-views ()
  (let ((root (current-root)))
    (loop for name in (fset:convert 'list (fset:domain root))
          for object = (values (fset:lookup root name))
          when (typep object 'view)
            collect (list :name name :query (view-query object)))))

(defun run-view (view)
  (apply #'select (view-query view)))

(define-replay :create-view (root name query)
  (fset:with root name (make-view name query)))

(define-replay :drop-view (root name)
  (fset:less root name))

(register-snapshot-section
 :views
 (lambda (root)
   (loop for name in (fset:convert 'list (fset:domain root))
         for object = (values (fset:lookup root name))
         when (typep object 'view)
           collect (list name (view-query object))))
 (lambda (root data)
   (loop for (name query) in data
         do (setf root (fset:with root name (make-view name query))))
   root))

;; A file keeps data only. A view that holds a lambda can't be saved.
(register-save-check
 (lambda (root)
   (loop for name in (fset:convert 'list (fset:domain root))
         for object = (values (fset:lookup root name))
         when (and (typep object 'view) (unsavable-p (view-query object)))
           do (error 'persistence-error
                     :message (format nil "Can't save view ~A: its query holds a function object. Use a function name."
                                      name)))))
