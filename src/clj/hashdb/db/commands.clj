(ns hashdb.db.commands
  (:require
   [clj-time.jdbc]
   [clojure.java.jdbc :as jdbc]
   [clojure.java.jdbc :as sql]
   [clojure.spec.alpha :as s]
   [hashdb.config :refer [env]]
   [hashdb.db.core :refer [*db*]]
   [hashdb.db.core :as cmd]
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.test.alpha :as stest2]
   [orchestra.spec.test]
   [mw.std :refer :all])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))

;; uuid? is the underlaying, not sure how this is mapped to db

(s/def ::uuid-str (s/and string? #(<= (count %) 36)))
(s/fdef uuid
        :args (s/cat)
        :ret  ::uuid-str)

(defn uuid []
  "Return a uuid as string."
  (str (java.util.UUID/randomUUID)))

(defn now []
  "Return now in UTC."
  (new java.util.Date))

#_
(defn nn
  "not-nil: abort if value nil"
  [v]
  (assert (some? v))
  v)

;; (s/fdef fsome
;;         :args (s/cat :form (s/cat :f symbol? :arg1 some?)) ; cannot use fn? since not parsed as function yet.
;;         :ret (constantly true))

;; I cannot undef the above spec, so I did like this for now
(s/fdef fsome
        :args (constantly true)
        :ret  (constantly true))

;; should be renamed to fwhen, since replacement of when.
(defmacro fsome
  "Use like (fsome (f arg1)), which is same as (f arg1), except that if arg1 is nil, nil is returned."
  [[f arg1]]
  `(let [arg1# ~arg1]
     (when arg1# (~f arg1#))))


(s/def ::datetime #(instance? java.util.Date %))

(s/def ::id ::uuid-str)
(s/def ::entity-str (s/nilable (s/and string? #(<= (count %) 36))))
(s/def ::entity (s/nilable keyword?))
(s/def ::deleted int?)
(s/def ::data (s/map-of keyword? (s/or :int int? :float float? :string string? :nil nil? :datetime ::datetime)))
(s/def ::before map?)
(s/def ::after map?)
(s/def ::updated (constantly true))     ; should check that time
(s/def ::version (s/and int? pos?))
(s/def ::parent (s/and int? #(>= % 0)))
(s/def ::is_merge int?)
(s/def ::userid (s/nilable ::uuid-str))
(s/def ::sessionid (s/nilable ::uuid-str))
(s/def ::comment (s/nilable (s/and string? #(<= (count %) 999))))

(s/def ::stored-latest
  (s/keys :req-un [::id ::updated ::version]
          :opt-un [::entity]))

(s/def ::stored-history
  (s/keys :req-un [::id ::deleted ::before ::after ::updated ::version ::parent]
          :opt-un [::entity ::is_merge ::userid ::sessionid ::comment]))

(s/def ::stored-history-short
  (s/keys :req-un [::id ::deleted ::updated ::version ::parent]
          :opt-un [::entity ::is_merge ::userid ::sessionid ::comment]))

(s/fdef create
        :args (s/cat :m ::data)
        :ret ::stored-latest)

(defn create
  "Insert map `m` into db.
   If `:id` is in `m`, use it, otherwise create one.
   Return the map incl the potentially created id."
  [m]
  (let [id0        (or (:id m) (uuid))
        entity     (:entity m)
        entity-str (fsome (pr-str entity))
        now0       (now)
        version    1
        m0         (into m {:id id0 :updated now0 :version version})
        m          (if entity (assoc m0 :entity entity) m0)
        data       (pr-str m)]
    (sql/with-db-transaction [conn *db*]
      (cmd/create-latest! conn
                          {:id id0 :entity entity-str :data data :updated now0 :parent 0 :version version})
      (cmd/create-history! conn
                           {:id       id0,  :entity    entity-str, :deleted 0, :before "{}", :after data,
                            :updated  now0, :version   version,    :parent  0,
                            :is_merge 0,
                            :userid   nil,  :sessionid nil,        :comment nil})
      m)))

(defn- verify-row-in-sync
  "Make sure db `row` is in sync with `m` from the :data column.
   Currently, only :id and :version are verified, since if these arr
   wrong, it is a catastrophic error."
  [row m]
  (assert (and (= (:id row)(:id m))
               (= (:version row)(:version m))
               (if (:entity row)
                 (= (:entity row)(pr-str (:entity m)))
                 (nil? (:entity m))))
          "id, entity, and/or version and data columns in table latest are not in sync.")
  m)

(defn- try-get-internal
  "Return map at `id`, null if not found."
  [id row]
  (let [row (cmd/get-latest {:id id})
        m   (clojure.edn/read-string (:data row))]
    (when row (assert (and (= id (:id row)))
                    "id and/or version and data columns in table latest are not in sync."))
    (when m (verify-row-in-sync row m))
    m))


(s/fdef try-get
        :args (s/cat :id string?)
        :ret (s/nilable ::stored-latest))

(defn try-get
  "Return map at `id`, null if not found."
  [id]
  (try-get-internal id (cmd/get-latest {:id id})))


(s/fdef get
        :args (s/cat :id string?)
        :ret ::stored-latest)

(defn get
  "Return map at `id`, exception if not found."
  [id]
  (let [m (try-get id)]
    (if m m (throw (ex-info (str "Row with id " id " missing!")
                            {:id id})))))


(s/fdef select-all
        :args (s/cat)
        :ret (s/* ::stored-latest))

(defn select-all
  "Return all rows."
  []
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest)))


(s/fdef select-all-by-entity
        :args (s/cat :entity keyword?)
        :ret (s/* ::stored-latest))

(defn select-all-by-entity
  "Return all rows for a given `entity`."
  [entity]
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest-by-entity {:entity (fsome (pr-str entity))})))


(s/fdef select-all-nil-entity
        :args (s/cat)
        :ret (s/* ::stored-latest))

(defn select-all-nil-entity
  "Return all rows for unknown `entity`."
  []
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest-null-entity {})))


(s/fdef update
        :args (s/cat :m ::stored-latest :changes ::data)
        :ret ::stored-latest)

(defn update
  "Map `m` is the one currently stored in the db.
   The map `changes` contains the fields that should be updated.
   Return the new map, or throw exception if update fails."
  [m changes]
  (assert (nil? (:id changes)) "Changing :id is not allowed!")
  (assert (nil? (:entity changes)) "Changing :entity is not allowed!")
  (assert (and (some? (:version m))(>= (:version m) 1)) "Version 0 should be create:d!")
  (let [id         (nn (:id m))
        parent     (nn (:version m))
        ;; for the non-acid update, (inc parent) will not do, maybe I need to using timestamp again
        version    (inc parent)
        updated    (now)
        before     (select-keys m (keys changes))
        data       (into (into m changes) {:updated updated :version version})
        data-str   (pr-str data)
        entity-str (fsome (pr-str (:entity m)))]
    (sql/with-db-transaction [conn *db*]
      (let [affected (cmd/update-latest! conn {:id id :parent parent :updated updated :version version :data data-str})]
        (cond (= 0 affected) (throw (ex-info (str "Row " id " has been updated since read " parent)
                                             {:id id :updated parent}))
              (> affected 1) (throw (ex-info (str "Row " id " existed several times in db.")
                                             {:id id :updated parent})))

        ;; why not in a transaction? since insert, it cannot fail.
        ;; and for non-acid-update, the history is the long-term truth, not the latest entry
        (cmd/create-history! conn
                             {:id       id,              :entity    entity-str, :deleted 0,
                              :before   (pr-str before), :after     (pr-str changes),
                              :updated  updated,         :version   version,    :parent  parent,
                              :is_merge 0,
                              :userid   nil,             :sessionid nil,        :comment nil}))

      data)))


;; (into {} (clojure.set/difference (into #{} {:a 1, :b 2, :d 4}) (into #{} {:a 2, :c 3, :d 4})))
;; => {:b 2, :a 1}

(s/fdef map-difference
        :args (s/cat :m-new map? :m_old map?)
        :ret map?)

(defn- map-difference
  "Remove all kv-pairs in `m-new` that already exists in `m-old`."
  [m-new m-old]
  (into {} (clojure.set/difference (into #{} m-new) (into #{} m-old))))


(s/fdef update-diff
        :args (s/cat :m ::stored-latest :changes ::data)
        :ret ::stored-latest)


(defn update-diff
  "Find the differences made and then update db."
  [m-old m-new]
  (let [changes (map-difference m-new m-old)]
    (update m-old changes)))


(s/fdef delete
        :args (s/cat :m ::data)
        :ret nil?)

(defn delete
  "Delete the row for `m`.
   We do not care if anyone has updated or deleted the row just before.
   Will leave a perfect history."
  [m]
  (assert (and (:id m) (:version m)) ":id & :version is minimum for delete.")
  (sql/with-db-transaction [conn *db*]
    (let [affected   (cmd/delete-latest! conn m)
          entity-str (fsome (pr-str (:entity m)))]
      (cmd/create-history! conn
                           {:id       (:id m),    :entity    entity-str,         :deleted 1,
                            :before   (pr-str m), :after     "{}",
                            :updated  (now),      :version   (inc (:version m)), :parent  (:version m),
                            :is_merge 0,
                            :userid   nil,        :sessionid nil,                :comment nil})
      nil)))


(s/fdef delete-by-id-with-minimum-history
        :args (s/cat :id string?)
        :ret nil?)

(defn delete-by-id-with-minimum-history
  "Delete the row `id`.
   The last history entry will not be optimal.
   Workaround, read the record first, use delete-by-id.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (sql/with-db-transaction [conn *db*]
    (let [affected (cmd/delete-latest! conn {:id id})]
      (cmd/create-history! conn
                           {:id       id,    :entity    nil,        :deleted 1,
                            :before   "{}",  :after     "{}",
                            :updated  (now), :version   2000000001, :parent  2000000000,
                            :is_merge 0,
                            :userid   nil,   :sessionid nil,        :comment nil})
      nil)))


(s/fdef delete-by-id
        :args (s/cat :id ::uuid-str)
        :ret nil?)

(defn delete-by-id
  "Delete the row `id`.
   Make sure history is perfect by reading the record first.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (if-let [m (try-get id)]
    (sql/with-db-transaction [conn *db*]
      (let [affected (cmd/delete-latest! conn {:id (:id m)})
            version  (:version m)]
        (cmd/create-history! conn
                             {:id       (:id m),    :entity    (fsome (pr-str (:entity m))), :deleted 1,
                              :before   (pr-str m), :after     "{}",
                              :updated  (now),      :version   (inc version),                :parent  version,
                              :is_merge 0,
                              :userid   nil,        :sessionid nil,                          :comment nil})
        nil))))

(defn- deserialize-history
  "Deserialize the :before and :after keys of `m`."
  [m]
  (into m {:after (clojure.edn/read-string (:after m))
           :before (clojure.edn/read-string (:before m))
           :entity (fsome (clojure.edn/read-string (:entity m)))}))


(s/fdef history
        :args (s/cat :id ::uuid-str)
        :ret  (s/* ::stored-history))

(defn history
  "Get the complete history for `id`."
  [id]
  (map deserialize-history (cmd/select-history {:id id})))


(s/fdef history-by-entity
        :args (s/cat :entity keyword?)
        :ret  (s/* ::stored-history))


(defn history-by-entity
  "Get the complete history for all entities of `entity`."
  [entity]
  (map deserialize-history (cmd/select-history-by-entity {:entity (fsome (pr-str entity))})))


(s/fdef history-nill-entity
        :args (s/cat)
        :ret  (s/* ::stored-history))

(defn history-nil-entity
  "Get the complete history for all entities of `entity`."
  []
  (map deserialize-history (cmd/select-history-null-entity {})))


;; this operation is most likely much faster for long histories since:
;; 1. no deserialization
;; 2. InnoDB puts only 767 bytes of a TEXT or BLOB inline, the rest goes into some other block.
;;    This is a compromise that sometimes helps, sometimes hurts performance.
(s/fdef history-short
        :args (s/cat :id string?)
        :ret  (s/* ::stored-history-short))

(defn history-short
  "Get the complete history for `id`, but do not read :before and :after.
   Used to present who changed anything for this object."
  [id]
  (cmd/select-history-short {:id id}))

(orchestra.spec.test/instrument)
