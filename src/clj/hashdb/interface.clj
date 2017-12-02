(ns hashdb.interface
  (:require
   [hashdb.db.commands :as commands])
  (:refer-clojure :exclude [get]))

;; (:doc (meta #'hashdb.db.commands/create!))
;; "Insert map `m` into db.\n   If `:id` is in `m`, use it, otherwise create one.\n   Return the map incl the potentially created id."


;; https://stackoverflow.com/questions/47596935/copy-arglists-into-a-clojure-def/47597766#47597766

(defmacro idef
  [fname]
  (let [sym (symbol (str "hashdb.db.commands/" fname))
        metas (meta (find-var sym))
        arglists (:arglists metas)
        doc (:doc metas)]
    `(def ~(with-meta fname {:doc doc :arglists `(quote ~arglists)}) ~sym)))

(idef single-tenant-mode)
(idef reset-single-tenant-mode)
(idef get-tenant)
(idef set-*indexes-fn*)
(idef create!)
(idef try-get)
(idef get)
(idef select-all)
(idef select-all-by-entity)
(idef select-all-nil-entity)
(idef select-by)
(idef select-by-global)
(idef update!)
(idef update-diff!)
(idef delete!)
(idef delete-by-id-with-minimum-history!)
(idef delete-by-id!)
(idef history)
(idef history-by-entity)
(idef history-nil-entity)
(idef history-short)
(idef clear-database)
(idef create-database-tables)
