(ns hashdb.db.commands
  (:require
   [clojure.spec.alpha :as s]
   [clj-time.jdbc]
   [clojure.java.jdbc :as jdbc]
   [clojure.java.jdbc :as sql]
   [hashdb.config :refer [env]]
   [hashdb.db.core :as cmd])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))


(defn uuid []
  (str (java.util.UUID/randomUUID)))

(defn now []
  (new java.util.Date))

(defn nn
  "not-nil: abort if value nil"
  [v]
  (assert (some? v))
  v)


(s/fdef create
        :args (s/cat :m map?)
        :ret map?)

(defn create
  "Insert map `m`.
   If `:id` is in `m`, use it, otherwise create one.
   Return the map incl the potentially created id."
  [m]
  (let [id0 (or (:id m) (uuid))
        entity (or (:entity m) :unknown)
        entity-str (pr-str entity)
        now0 (now)
        version 1
        m (into m {:id id0 :updated now0 :version version :entity entity})
        data (pr-str m)]
    (cmd/create-latest! {:id id0 :entity entity-str :data data :updated now0 :parent 0 :version version})
    (cmd/create-history! {:id id0, :entity entity-str, :deleted 0, :before "{}", :after data,
                          :updated now0, :version version, :parent 0,
                          :is_merge 0,
                          :userid nil, :sessionid nil, :comment nil})
    m))

(defn- verify-row-in-sync
  "Make sure db `row` is in sync with `m` from the :data column.
   Currently, only :id and :version are verified, since if these arr
   wrong, it is a catastrophic error."
  [row m]
  (assert (and (= (:id row)(:id m))
               (= (:version row)(:version m))
               (= (:entity row)(pr-str (:entity m))))
          "id, entity, and/or version and data columns in table latest are not in sync.")
  m)

(defn- try-get-internal
  "Return map at `id`, null if not found."
  [id row]
  (let [row (cmd/get-latest {:id id})
        m (clojure.edn/read-string (:data row))]
    (when row (assert (and (= id (:id row)))
                    "id and/or version and data columns in table latest are not in sync."))
    (when m (verify-row-in-sync row m))
    m))


(s/fdef try-get
        :args (s/cat :id string?)
        :ret (s/nilable map?))

(defn try-get
  "Return map at `id`, null if not found."
  [id]
  (try-get-internal id (cmd/get-latest {:id id})))


(s/fdef get
        :args (s/cat :id string?)
        :ret map?)

(defn get
  "Return map at `id`, exception if not found."
  [id]
  (let [m (try-get id)]
    (if m m (throw (ex-info (str "Row with id " id " missing!")
                            {:id id})))))


(s/fdef select-all
        :args (s/cat)
        :ret seq?)

(defn select-all
  "Return all rows."
  []
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest)))


(s/fdef select-all-by-entity
        :args (s/cat :entity keyword?)
        :ret seq?)

(defn select-all-by-entity
  "Return all rows for a given `entity`.
   :unknown are special cases."
  [entity]
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest-by-entity {:entity (pr-str entity)})))


(s/fdef update
        :args (s/cat :m map? :changes map?)
        :ret map?)

(defn update
  "Map `m` is the one currently stored in the db.
   The map `changes` contains the fields that should be updated.
   Return the new map, or throw exception if update fails."
  [m changes]
  (assert (nil? (:id changes)) "Changing :id is not allowed!")
  (assert (nil? (:entity changes)) "Changing :entity is not allowed!")
  (assert (and (some? (:version m))(>= (:version m) 1)) "Version 0 should be create:d!")
  (let [id (nn (:id m))
        parent (nn (:version m))
        ;; for the non-acid update, (inc parent) will not do, maybe I need to using timestamp again
        version (inc parent)
        updated (now)
        before (select-keys m (keys changes))
        data (into (into m changes) {:updated updated :version version})
        data-str (pr-str data)
        affected (cmd/update-latest! {:id id :parent parent :updated updated :version version :data data-str})]
    (cond (= 0 affected) (throw (ex-info (str "Row " id " has been updated since read " parent)
                                         {:id id :updated parent}))
          (> affected 1) (throw (ex-info (str "Row " id " existed several times in db.")
                                         {:id id :updated parent})))

    ;; why not in a transaction? since insert, it cannot fail.
    ;; and for non-acid-update, the history is the long-term truth, not the latest entry
    (cmd/create-history! {:id id, :entity (pr-str (:entity m)), :deleted 0,
                          :before (pr-str before), :after (pr-str changes),
                          :updated updated, :version version, :parent parent,
                          :is_merge 0,
                          :userid nil, :sessionid nil, :comment nil})

    data))

(s/fdef delete
        :args (s/cat :m map?)
        :ret nil?)

(defn delete
  "Delete the row for `m`.
   We do not care if anyone has updated or deleted the row just before.
   Will leave a perfect history."
  [m]
  (assert (and (:id m) (:version m)) ":id & :version is minimum for delete.")
  (let [affected (cmd/delete-latest! m)]
    (cmd/create-history! {:id (:id m), :entity (pr-str (:entity m)), :deleted 1,
                          :before (pr-str m), :after "{}",
                          :updated (now), :version (inc (:version m)), :parent (:version m),
                          :is_merge 0,
                          :userid nil, :sessionid nil, :comment nil})))


(s/fdef delete-by-id-with-minimum-history
        :args (s/cat :id string?)
        :ret nil?)

(defn delete-by-id-with-minimum-history
  "Delete the row `id`.
   The last history entry will not be optimal.
   Workaround, read the record first, use delete-by-id.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (let [affected (cmd/delete-latest! {:id id})]
    (cmd/create-history! {:id id, :entity ":unknown", :deleted 1,
                          :before "{}", :after "{}",
                          :updated (now), :version 2000000001, :parent 2000000000,
                          :is_merge 0,
                          :userid nil, :sessionid nil, :comment nil})
    nil))


(s/fdef delete-by-id
        :args (s/cat :id string?)
        :ret nil?)

(defn delete-by-id
  "Delete the row `id`.
   Make sure history is perfect by reading the record first.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (if-let [m (try-get id)]
    (let [affected (cmd/delete-latest! {:id (:id m)})
          version (:version m)]
      (cmd/create-history! {:id (:id m), :entity (pr-str (:entity m)), :deleted 1,
                            :before (pr-str m), :after "{}",
                            :updated (now), :version (inc version), :parent version,
                            :is_merge 0,
                            :userid nil, :sessionid nil, :comment nil})
      nil)))

(defn- deserialize-history
  "Deserialize the :before and :after keys of `m`."
  [m]
  (into m {:after (clojure.edn/read-string (:after m))
           :before (clojure.edn/read-string (:before m))
           :entity (clojure.edn/read-string (:entity m))}))


(s/fdef history
        :args (s/cat :id string?)
        :ret seq?)

(defn history
  "Get the complete history for `id`."
  [id]
  (map deserialize-history (cmd/select-history {:id id})))


(s/fdef history-by-entity
        :args (s/cat :entity keyword?)
        :ret seq?)

(defn history-by-entity
  "Get the complete history for all entities of `entity`."
  [entity]
  (map deserialize-history (cmd/select-history-by-entity {:entity (pr-str entity)})))


;; this operation is most likely much faster for long histories since:
;; 1. no deserialization
;; 2. InnoDB puts only 767 bytes of a TEXT or BLOB inline, the rest goes into some other block.
;;    This is a compromise that sometimes helps, sometimes hurts performance.
(s/fdef history-short
        :args (s/cat :id string?)
        :ret seq?)

(defn history-short
  "Get the complete history for `id`, but do not read :before and :after.
   Used to present who changed anything for this object."
  [id]
  (cmd/select-history-short {:id id}))
