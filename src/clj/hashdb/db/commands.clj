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
   [mw.std :refer :all]
   [qbits.tardis :as qbits])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))


;;; TODO: BUG: I have not properly thought about when to delete index-entries
;;; TODO: BUG: I will do it too often, unless.
;;; The original idea was like this: I send in a map + only the keys that needs to be changed
;;; But then the question is how to delete map-entries? Most logical is to set them to nil.

;;; TODO: add :k column to string_index, and use entity for entity, since it will
;;; TODO: improve lookups if same key used in many different entities (like SSS's frm)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; mysql tips:
;;; 1. subqueries are slow, but there is a trick:
;;;    https://stackoverflow.com/questions/6135376/mysql-select-where-field-in-subquery-extremely-slow-why
;;; 2. What is done i v6? Also shows different ways of doing it.
;;;    https://www.scribd.com/document/2546837/New-Subquery-Optimizations-In-MySQL-6

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; generic functions

;; (into {} (clojure.set/difference (into #{} {:a 1, :b 2, :d 4}) (into #{} {:a 2, :c 3, :d 4})))
;; => {:b 2, :a 1}

(s/fdef map-difference
        :args (s/cat :m-new map? :m_old map?)
        :ret map?)

(defn- map-difference
  "Remove all kv-pairs in `m-new` that already exists in `m-old`."
  [m-new m-old]
  (into {} (clojure.set/difference (into #{} m-new) (into #{} m-old))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; some spec-helpers

;; defn since s/with-gen function
(defn dev-with-gen
  "Only generator in dev code."
  [spec gen-fn]
  (if (:dev env)
    (s/with-gen spec gen-fn)
    spec))

;; defmacro, since s/or macro
(defmacro dev-spec-or
  "In dev (dev-spec-or :a a :b b) same as (s/or :a a :b b), but in
   prod only a."
  [k1 pred1 k2 pred2]
  (if (:dev env)
    `(s/or ~k1 ~pred1 ~k2 ~pred2)
    pred1))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; specs for api and storage

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


;; (s/fdef fsome
;;         :args (s/cat :form (s/cat :f symbol? :arg1 some?)) ; cannot use fn? since not parsed as function yet.
;;         :ret any?)
;; tips: http://blog.klipse.tech/clojure/2016/10/10/defn-args.html

;; (s/def ::function-application (s/cat :f symbol? :arg1 any?))

;; I cannot undef the above spec, so I did like this for now
(s/fdef fsome
        :args any?
        :ret  any?)

;; (s/fdef fsome
;;         :args (s/cat :arg1 ::function-application)
;;         :ret  any?)

;; should be renamed to fwhen, since replacement of when.
(defmacro fsome
  "Use like (fsome (f arg1)), which is same as (f arg1), except that if arg1 is nil, nil is returned."
  [[f arg1]]
  ;; instead of using s/fdef, check clojure spec inside macro
  (assert (and (s/valid? symbol? f)(s/valid? any? arg1)))
  `(let [arg1# ~arg1]
     (when arg1# (~f arg1#))))


(s/def ::datetime (dev-with-gen #(or
                                  ;; mysql select returns joda-time
                                  (instance? org.joda.time.DateTime %)
                                  (instance? java.util.Date %))
                                #(s/gen #{#inst "2000-01-01T00:00:00.000-00:00"})))

(s/def ::id ::uuid-str)
(s/def ::entity-str (s/and string? #(<= (count %) 36)))
(s/def ::entity keyword?)
(s/def ::deleted boolean?)
(s/def ::before map?)
(s/def ::after map?)
(s/def ::updated ::datetime)
(s/def ::version (s/and int? pos?))
(s/def ::parent (s/and int? #(>= % 0)))
(s/def ::is_merge boolean?)
(s/def ::userid (s/nilable ::uuid-str))
(s/def ::sessionid (s/nilable ::uuid-str))
(s/def ::comment (s/nilable (s/and string? #(<= (count %) 999))))

;; the toplevel in our data should be a map with keywords. The rest we do
;; not care about. Do not know is there is a any-spec that generates.
;; YES any? solves it EXCEPT DATETIME
;; (s/def ::myany
;;   (s/or :int int?
;;         :float float?
;;         :string string?
;;         :nil nil?
;;         :datetime ::datetime
;;         :boolean boolean?
;;         :set set?
;;         :map map?
;;         :vector vector?
;;         :seq seq?))

;; Added some known keys, which I can use for indexing testing

;; trial 1 (but then I missed the types of the index)
;; (s/def ::key
;;   (dev-spec-or :normal keyword? :dev #{:a :b :c :d :e}))
;; (s/def ::data
;;   (s/map-of ::key any?))
;;
;; trial 2
;; use s/merge, which merges maps.
;; In order to sync keyword with types, you need to use s/keys
;; SUCCESS!

(if-not (:dev env)
  (s/def ::data
    (s/map-of keyword? any?))
  (do
    (s/def ::s1 string?)
    (s/def ::s2 string?)
    (s/def ::s3 string?)
    (s/def ::s4 string?)
    ;; (s/def ::i1 int?)
    ;; (s/def ::i2 int?)
    (s/def ::indexed-data
      (s/keys :opt-un [::s1 ::s2 ::s3 ::s4
                       ;; ::i1 ::i2
                       ::s4]))
    (s/def ::data
      (s/merge (s/map-of keyword? any?) ::indexed-data))))


(s/def ::stored-latest
  (s/keys :req-un [::id ::entity ::updated ::version]
          :opt-un []))

(s/def ::stored-history
  (s/keys :req-un [::id ::entity ::deleted ::before ::after ::updated ::version ::parent]
          :opt-un [::is_merge ::userid ::sessionid ::comment]))

(s/def ::stored-history-short
  (s/keys :req-un [::id  ::entity ::deleted ::updated ::version ::parent]
          :opt-un [::is_merge ::userid ::sessionid ::comment]))

(s/def ::idx-type #{:string :long})

(s/def ::idx-info
  (s/tuple (s/map-of keyword? ::idx-type)
           (s/coll-of keyword? :into #{})))

(s/fdef indexes
        :args (s/cat :m ::data)
        :ret  ::idx-info)

;; TODO: implement a function that returns this list.
;; TODO: For now, it is hard-coded
(defn indexes
  "Depending on the entity and other fields in `m`, return the indexes.
   Only non-nil values will be indexed."
  [m]
  (let [raw {:s1 :string, :s2 :string, :s3 :string, :s4 :string,
             :i1 :long, :i2 :long}]
    [raw (into #{} (map key raw))]))

;; for edn, we need to use pr-str
;; :static true is actually a no-op, it is dynamic that is needed.
;;(def ^{:static true} str-edn pr-str)
(def str-edn pr-str)

;; but for str-index, we do not want quotes around and similar
(def str-index str)

;; mattias premature performance optimization :-)
(def empty-map-edn (str-edn {}))

;; algorithm #1 for update-indexes!
;; 1. keep only fields in before and after that have indexes
;; 2. for the difference, 3 options:
;; 3a. nil or non-existent in before, exists in after => insert
;; 3b. exists in before, nil or non-existent in after => delete
;; 3c. exists in before, exists in after => update
;;
;; optimization, if I want to use mult-insert, it is best if we loop per type,
;; i.e. first for :string, then for :int

(defn mapify-values
  "(mapify-values {:1 [[:a :b][:c :d]] :2 [[:e :f]]}))
   ==> {:1 {:a :b, :c :d}, :2 {:e :f}}"
  [m]
  (into {} (map (fn [[k v]] [k (into {} v)]) m)))

(s/fdef keep-difference-by-type
        :args (s/cat :changes ::data :idx-info ::idx-info)
        :ret  ::data)

(defn keep-difference-by-type
  "Only keep the changes keys that have indexes.
   Group these by type, e.g. :string :int..."
  [changes [idx-types idx-keys]]
  (if-not changes {}
          (let [changes-relevant (select-keys changes idx-keys)
                changes-grouped  (mapify-values (group-by #(idx-types (key %)) changes-relevant))]
            changes-grouped)))


(defn- update-indexes-one-type-create!
  [conn before typ changes id entity]
  (assert (empty? before))
  (if (= :string typ)
    ;; (sort to avoid deadlock, by always adding stuff in the same order
    (doseq [[k v] (sort changes)]
      ;; no point in adding nil:s to index
      (if v (cmd/create-string-index!
             conn
             {:id id, :entity (str-edn entity), :k (str-edn k),
              :index_data (str-index v)})))))

(defn- update-indexes-one-type-update!
  [conn before changes typ id entity]
  (let [keys-before (set (keys before))
        keys-changes0 (set (keys changes))
        disappeared(clojure.set/difference keys-before keys-changes0)
        _ (assert (empty? disappeared) (str "Illegal update, the following keys needs to be part of changes" (str disappeared)))
        tmp (group-by #(nil? (second %)) changes)
        map-changes (clojure.core/get tmp false)
        map-deleted (clojure.core/get tmp true)
        key-changes (keys map-changes)
        should-be-updated (clojure.set/intersection (set key-changes) keys-before)
        should-be-created (clojure.set/difference (set (keys map-changes)) keys-before)]
    (when (= :string typ)
      ;; Sort to minimize risk of deadlock
      ;; MySQLTransactionRollbackException Deadlock found when trying to get lock; try restarting transaction  com.mysql.cj.jdbc.exceptions.SQLError.createSQLException (SQLError.java:539)
      ;; Todo: If it still happends, I have to sort delete+create+update according to :k
      (doseq [[k v] (sort map-deleted)]
        ;; v = nil means deleted
        (assert (nil? v))
        (let [res (cmd/delete-single-string-index! conn {:id id, :entity (str-edn entity), :k (str-edn k)})]
          (assert (= 1 res))))

      (doseq [k (sort should-be-created)]
        ;; no point in adding nil:s to index, so should never happen, even if has nil value
        (assert (k changes))
        (let [res (cmd/create-string-index! conn {:id id, :entity (str-edn entity), :k (str-edn k)
                                                  :index_data (str-index (k changes))})]
          (assert (= 1 res))))
      (doseq [k (sort should-be-updated)]
        ;; update to nil, same as deleting (but this case should not happen
        ;; since then changes will not fulfil ::data)
        (assert (k changes))
        (cmd/update-string-index! conn {:id id, :entity (str-edn entity), :k (str-edn k)
                                        :index_data (str-index (k changes))}))))
  nil)

;; for delete, we can just delete all index entries at once
;; so actually unnecessary to see which exact keywords are indexed,
;; just which type of indexes there are (no point deleting int-indexes
;; there are none)
(defn- update-indexes-one-type-delete!
  [conn changes typ id]
  (assert (empty? changes))
  (if (= :string typ)
    (cmd/delete-string-index! conn {:id id}))
  nil)


(s/def ::sql-op #{:create :update :delete})

(s/fdef update-indexes-one-type!
        :args (s/cat :conn any? :sql-op ::sql-op :typ ::idx-type :m ::data
                     :before (s/nilable ::data) :changes (s/nilable ::data))
        :ret nil?)

(defn update-indexes-one-type!
  [conn sql-op typ m before changes]
  (let [id (:id m)
        entity (:entity m)]
    (cond (= :create sql-op)
          (update-indexes-one-type-create! conn before typ changes id entity)
          (= :update sql-op)
          (update-indexes-one-type-update! conn before changes typ id entity)
          (= :delete sql-op)
          (update-indexes-one-type-delete! conn changes typ id))))


(s/fdef update-indexes!
        :args (s/cat :conn any? :sql-op ::sql-op :m ::data
                     :before ::data :changes (s/nilable ::data))
        :ret nil?)

(defn update-indexes!
  "Update all index entries that have been changed.
   If it is a new object, before should be empty.
   If it is a delete, changes should be nil."
  [conn sql-op m before changes]
  (let [idx-info         (indexes m)
        before-relevant  (keep-difference-by-type before idx-info)
        changes-relevant (keep-difference-by-type changes idx-info)]
    (doseq [typ [:string :long]]
      (update-indexes-one-type! conn sql-op typ m
                                (typ before-relevant) (typ changes-relevant)))))


(s/fdef delete-indexes-without-data!
        :args (s/cat :conn any? :id ::id)
        :ret nil?)

;; do not know which indexes to delete, i.e. has to look everywhere
(defn delete-indexes-without-data!
  "Delete all index entries."
  [conn id]
  (doseq [typ [:string :long]]
    (update-indexes-one-type-delete! conn {} typ id)))


(s/fdef create!
        :args (s/cat :m ::data)
        :ret ::stored-latest)

(defn create!
  "Insert map `m` into db.
   If `:id` is in `m`, use it, otherwise create one.
   Return the map incl the potentially created id."
  [m]
  (let [id0        (or (:id m) (uuid))
        entity     (or (:entity m) :unknown)
        entity-str (str-edn entity)
        now0       (now)
        version    1
        m          (into m {:id id0 :updated now0 :version version :entity entity})
        data       (str-edn m)]
    (sql/with-db-transaction [conn *db*]
      (cmd/create-latest! conn
                          {:id id0 :entity entity-str :data data :updated now0 :parent 0 :version version})
      (cmd/create-history! conn
                           {:id       id0,  :entity    entity-str, :deleted 0, :before empty-map-edn, :after data,
                            :updated  now0, :version   version,    :parent  0,
                            :is_merge 0,
                            :userid   nil,  :sessionid nil,        :comment nil})
      (update-indexes! conn :create m {} m)
      m)))


(defn- verify-row-in-sync
  "Make sure db `row` is in sync with `m` from the :data column.
   Currently, only :id and :version are verified, since if these arr
   wrong, it is a catastrophic error."
  [row m]
  (assert (and (= (:id row)(:id m))
               (= (:version row)(:version m))
               (= (:entity row)(str-edn (:entity m))))
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
        :args (s/cat :id ::id)
        :ret (s/nilable ::stored-latest))

(defn try-get
  "Return map at `id`, null if not found."
  [id]
  (try-get-internal id (cmd/get-latest {:id id})))


(s/fdef get
        :args (s/cat :id ::id)
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
        :args (s/cat :entity ::entity)
        :ret (s/* ::stored-latest))

(defn select-all-by-entity
  "Return all rows for a given `entity`."
  [entity]
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest-by-entity {:entity (str-edn entity)})))


(s/fdef select-all-nil-entity
        :args (s/cat)
        :ret (s/* ::stored-latest))

(defn select-all-nil-entity
  "Return all rows for unknown `entity`."
  []
  (select-all-nil-entity :unknown))


(s/fdef update!
        :args (s/cat :m ::stored-latest :changes ::data)
        :ret ::stored-latest)

(defn update!
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
        data-str   (str-edn data)
        entity-str (str-edn (:entity m))]
    (sql/with-db-transaction [conn *db*]
      (let [affected (cmd/update-latest! conn {:id id :parent parent :updated updated :version version :data data-str})]
        (cond (= 0 affected) (throw (ex-info (str "Row " id " has been updated since read " parent)
                                             {:id id :updated parent}))
              (> affected 1) (throw (ex-info (str "Row " id " existed several times in db.")
                                             {:id id :updated parent})))

        ;; why not in a transaction? since insert, it cannot fail.
        ;; and for non-acid-update, the history is the long-term truth, not the latest entry
        (cmd/create-history! conn
                             {:id       id,               :entity    entity-str, :deleted 0,
                              :before   (str-edn before), :after     (str-edn changes),
                              :updated  updated,          :version   version,    :parent  parent,
                              :is_merge 0,
                              :userid   nil,              :sessionid nil,        :comment nil})
        (update-indexes! conn :update m before changes))
      data)))



(s/fdef update-diff!
        :args (s/cat :m ::stored-latest :changes ::data)
        :ret ::stored-latest)

(defn update-diff!
  "Find the differences made and then update db."
  [m-old m-new]
  (let [changes (map-difference m-new m-old)]
    (update! m-old changes)))


(s/fdef delete!
        :args (s/cat :m ::data)
        :ret nil?)

(defn delete!
  "Delete the row for `m`.
   We do not care if anyone has updated or deleted the row just before.
   Will leave a perfect history."
  [m]
  (assert (and (:id m) (:version m)) ":id & :version is minimum for delete.")
  (sql/with-db-transaction [conn *db*]
    (let [affected   (cmd/delete-latest! conn m)
          entity-str (str-edn (:entity m))]
      (cmd/create-history! conn
                           {:id       (:id m),     :entity    entity-str,         :deleted 1,
                            :before   (str-edn m), :after     empty-map-edn,
                            :updated  (now),       :version   (inc (:version m)), :parent  (:version m),
                            :is_merge 0,
                            :userid   nil,         :sessionid nil,                :comment nil})
      (update-indexes! conn :delete m m {})
      nil)))


(s/fdef delete-by-id-with-minimum-history!
        :args (s/cat :id ::id)
        :ret nil?)

(defn delete-by-id-with-minimum-history!
  "Delete the row `id`.
   The last history entry will not be optimal.
   Workaround, read the record first, use delete-by-id.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (sql/with-db-transaction [conn *db*]
    (let [affected (cmd/delete-latest! conn {:id id})]
      (cmd/create-history! conn
                           {:id       id,            :entity    (str-edn :unknown), :deleted 1,
                            :before   empty-map-edn, :after     empty-map-edn,
                            :updated  (now),         :version   2000000001,         :parent  2000000000,
                            :is_merge 0,
                            :userid   nil,           :sessionid nil,                :comment nil})
      (delete-indexes-without-data! conn id)
      nil)))


(s/fdef delete-by-id!
        :args (s/cat :id ::uuid-str)
        :ret nil?)

(defn delete-by-id!
  "Delete the row `id`.
   Make sure history is perfect by reading the record first.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (if-let [m (try-get id)]
    (sql/with-db-transaction [conn *db*]
      (let [affected (cmd/delete-latest! conn {:id (:id m)})
            version  (:version m)]
        (cmd/create-history! conn
                             {:id       (:id m),     :entity    (str-edn (:entity m)), :deleted 1,
                              :before   (str-edn m), :after     empty-map-edn,
                              :updated  (now),       :version   (inc version),         :parent  version,
                              :is_merge 0,
                              :userid   nil,         :sessionid nil,                   :comment nil})
        (delete-indexes-without-data! conn id)
        nil))))

(defn- deserialize-history
  "Deserialize the :before and :after keys of `m`."
  [m]
  (into m {:after (clojure.edn/read-string (:after m))
           :before (clojure.edn/read-string (:before m))
           :entity (clojure.edn/read-string (:entity m))}))


(s/fdef history
        :args (s/cat :id ::uuid-str)
        :ret  (s/* ::stored-history))

(defn history
  "Get the complete history for `id`."
  [id]
  (map deserialize-history (cmd/select-history {:id id})))


(s/fdef history-by-entity
        :args (s/cat :entity ::entity)
        :ret  (s/* ::stored-history))

(defn history-by-entity
  "Get the complete history for all entities of `entity`."
  [entity]
  (map deserialize-history (cmd/select-history-by-entity {:entity (str-edn entity)})))


(s/fdef history-nil-entity
        :args (s/cat)
        :ret  (s/* ::stored-history))

(defn history-nil-entity
  "Get the complete history for all :unknown entities."
  []
  (history-by-entity :unknown))


;; this operation is most likely much faster for long histories since:
;; 1. no deserialization of :data
;; 2. InnoDB puts only 767 bytes of a TEXT or BLOB inline, the rest goes into some other block.
;;    This is a compromise that sometimes helps, sometimes hurts performance.
(s/fdef history-short
        :args (s/cat :id ::id)
        :ret  (s/* ::stored-history-short))

(defn history-short
  "Get the complete history for `id`, but do not read :before and :after.
   Used to present who changed anything for this object."
  [id]
  (map deserialize-history (cmd/select-history-short {:id id})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; validation of database
;;;
;;; step 1: ensure latest and index are in sync

(s/fdef verify-stored-data
        :args (s/cat :id ::id)
        :ret  boolean?)

(defn verify-stored-data
  "Make sure the `m` with `id` is correctly and completely stored.
   If not, print out warning and return false."
  [id]
  (let [m           (try-get id)
        idxs        (cmd/select-string-index {:id id})
        idxs-as-map (into {} (map (fn [idx] [(clojure.edn/read-string (:k idx))(:index_data idx)]) idxs))]
    (if m
      (let [idx-info    (indexes m)
            idx-values  (select-keys m (second idx-info))
            diff        (keep-difference-by-type m idx-info)]
        (and (or (zero? (count idxs)) (= (:entity m) (clojure.edn/read-string (:entity (first idxs)))))
             (= (count idxs) (count idxs-as-map))
             (= (count idxs) (count idx-values))
             (empty? (map-difference idxs-as-map idx-values))))
      (zero? (count idxs)))))


(defn verify-these
  "Verify these `ids`."
  [ids]
  (doseq [id ids]
    (when-not (verify-stored-data id)
      (println (str id " not ok")))))


(defn verify-all-stored-data
  "Read all database and verify it."
  []
  (let [ids (map :id (cmd/select-all-latest))]
    (verify-these ids)))

(orchestra.spec.test/instrument)
