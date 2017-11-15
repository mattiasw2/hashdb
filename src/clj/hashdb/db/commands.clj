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
   [clj-time.core :as t]
   [mw.std :refer :all]
   [qbits.tardis :as qbits])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Tenant
;;
;; The database is shared between different tenants. A tenant
;; should never see data from another tennant.
;; Normally, you should never look into the database without
;; knowing the tenant. The only exceptions are
;;  * You do not know the tenant yet, typically the login page.
;;  * It is a single tenant system, then set tennant to "*single*"
;;
;; Most operations are not ok unless *tenant* is non-nil.
;;
;;

(s/def ::tenant
  (s/or :single #{:single}       ;Single-tenant usage
        :global #{:global}       ;Any tenant allowed, really dangerous
        :tenant string?))        ;The current tenant


;; The only allowed top-level value of *tenant* is nil or :single,
;; where nil should never be read, if it is, something is wrong.
(def ^:dynamic *tenant* nil)


;; Needs to be function, since *tenant* cannot be changed from other ns
(defn single-tenant-mode
  "There is only one tenant."
  []
  (def ^:dynamic *tenant* :single))


(defn reset-single-tenant-mode
  "Step out of single tenant mode.
   Only needed for debugging and unit tests."
  []
  (def ^:dynamic *tenant* nil))


(defmacro with-tenant
  "Bind *tenant* to `tenant` and execute `forms`."
  [tenant & forms]
  `(binding [*tenant* ~tenant]
     ~@forms))


(s/fdef tenant->str
        :args (s/cat :tenant ::tenant)
        :ret  string?)


;; The tenant name for :single
(def SINGLE-TENANT "!")


;; Wouldn't it be better to use spec for checking below? Now, the
;; code is duplicated. It is as if some spec:s should always be
;; vaidated, not just during development.
(defn tenant->str
  "Return the `tenant` so that it can be stored in db."
  [tenant]
  (cond (= tenant :single) SINGLE-TENANT
        (= tenant :global) (throw (Exception. ":global tenant is never allowed to be stored in db"))
        (nil? tenant)      (throw (Exception. "tenant is never allowed to be nil."))
        (string? tenant)   tenant
        true               (throw (Exception. (str "'" tenant "' as tenant is not allowed.")))))


(s/fdef str->tenant
        :args (s/cat :tenant string?)
        :ret  ::tenant)

(defn str->tenant
  "Return the `str` as a proper tenant value."
  [str]
  (cond (= str SINGLE-TENANT) :single
        true                  str))


(s/fdef verify-tenant-read
        :args (s/cat :tenant ::tenant)
        :ret  boolean?)

(defn verify-tenant-read
  "Unless `*tenant*` is :global, `tenant` and `*tenant*` must be equal."
  [tenant]
  (if (= :global *tenant*)
    true
    (if (= tenant *tenant*) true
        (throw (Exception.
                (str "Read for Tenant '" tenant
                     "' is not allowed when *tenant* is '" *tenant* "'"))))))


(s/fdef verify-tenant-write
        :args (s/cat :tenant ::tenant)
        :ret  boolean?)

(defn verify-tenant-write
  "If `*tenant*` is :global, the writing is not allowed.
  `tenant` and `*tenant*` must be equal."
  [tenant]
  (if (or (nil? *tenant*)(= :global *tenant*)(nil? tenant))
    (throw (Exception.
            (str "Write for tenant '" tenant "' is not allowed when *tenant* is '"
                 *tenant* "'")))
    (if (= tenant *tenant*)
      true
      (throw (Exception. (str "Write for Tenant '" tenant
                              "' is not allowed when *tenant* is '" *tenant* "'"))))))


(s/fdef get-tenant
        :args (s/cat)
        :ret  ::tenant)

(defn get-tenant
  "Unless `*tenant*` is :global or nil, we can use `*tenant*` as tenant."
  []
  (if (or (nil? *tenant*) (= :global *tenant*))
    (throw (Exception. (str "Read without explicit tenant not allowed when *tenant* is '" *tenant* "'")))
    *tenant*))


(defn get-tenant-raw
  "If you just want to access *tenant*"
  []
  *tenant*)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; mySQL tips:
;;;
;;; 1. Subqueries are slow, but there is a trick:
;;;    https://stackoverflow.com/questions/6135376/mysql-select-where-field-in-subquery-extremely-slow-why
;;; 2. What is done in mySQL v6? Also shows different ways of doing it.
;;;    https://www.scribd.com/document/2546837/New-Subquery-Optimizations-In-MySQL-6

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; # Generic functions

;;     (into {} (clojure.set/difference
;;                  (into #{} {:a 1, :b 2, :d 4})
;;                  (into #{} {:a 2, :c 3, :d 4})
;;     => {:b 2, :a 1}

(s/fdef map-difference
        :args (s/cat :m-new map? :m_old map?)
        :ret map?)

(defn- map-difference
  "Remove all kv-pairs in `m-new` that already exists in `m-old`."
  [m-new m-old]
  (into {} (clojure.set/difference (into #{} m-new) (into #{} m-old))))


;; We use UUID stored as string as ID in the database.
;; However, we can also use uuid directly. It is most likely stored as binary in db.
;; Then uuid? can be used as spec.
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


;;     (mapify-values {:1 [[:a :b][:c :d]] :2 [[:e :f]]}))
;;     ==> {:1 {:a :b, :c :d}, :2 {:e :f}}"
(defn mapify-values
  "The values of the map `m` are map-like. Convert them into maps."
  [m]
  (into {} (map (fn [[k v]] [k (into {} v)]) m)))


;; For edn, we need to use pr-str
;; :static true is actually a no-op, it is dynamic that is needed.
;;(def ^{:static true} str-edn pr-str)
(def str-edn pr-str)


;; But for str-index, we do not want quotes around and similar.
(def str-index str)


;; Mattias premature performance optimization :-)
(def empty-map-edn (str-edn {}))


;; ## TODO: how to create an spec for fwhen?
;;
;; I cannot use fn? since not parsed as function yet.
;;
;;     (s/fdef fwhen
;;             :args (s/cat :form (s/cat :f symbol?
;;                                       :arg1 some?
;;             :ret any?)
;;
;; Tips: http://blog.klipse.tech/clojure/2016/10/10/defn-args.html
;;
;;     (s/def ::function-application (s/cat :f symbol? :arg1 any?))
;;
;; I cannot undef the above spec, so I did like this for now
;;
;;     (s/fdef fwhen
;;             :args (s/cat :arg1 ::function-application)
;;             :ret  any?)

(s/fdef fwhen
        :args any?
        :ret  any?)

(defmacro fwhen
  "Use like (fwhen (f arg1)), which is same as (f arg1), except that if arg1 is nil, nil is returned."
  [[f arg1]]
  ;; instead of using s/fdef, check clojure spec inside macro
  (assert (and (s/valid? symbol? f)(s/valid? any? arg1)))
  `(let [arg1# ~arg1]
     (when arg1# (~f arg1#))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Some SPEC-helpers

;; defn since s/with-gen is a function function
(defn dev-with-gen
  "Only generator in dev code."
  [spec gen-fn]
  (if (:dev env)
    (s/with-gen spec gen-fn)
    spec))

;; defmacro, since s/or is a macro
(defmacro dev-spec-or
  "In dev (dev-spec-or :a a :b b) same as (s/or :a a :b b), but in
   prod only a."
  [k1 pred1 k2 pred2]
  (if (:dev env)
    `(s/or ~k1 ~pred1 ~k2 ~pred2)
    pred1))

;; ## Tip: s/form can be used to interpret SPECs
;;     (s/form ::indexed-data)
;;     =>
;;     (clojure.spec.alpha/keys
;;      :opt-un [:hashdb.db.commands/s1
;;               :hashdb.db.commands/s2
;;               :hashdb.db.commands/s3
;;               :hashdb.db.commands/s4
;;               :hashdb.db.commands/s4])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # SPECs for api and storage
;;


;; ## ::short-keyword always  has between 1 and 4 chars!
;;
;;     (gen/sample
;;      (gen/fmap
;;       #(keyword (substring+ % 0 4))
;;       (gen/string-alphanumeric)))
;;     => (: :n : :mi0 :1 :90G : :qDAv :5VDc :11)
(s/def ::short-keyword
  (s/with-gen keyword?
    #(gen/fmap (fn [k] (keyword (let [str (substring+ k 0 4)]
                                  (if (empty? str) "s1" str))))
               (gen/string-alphanumeric))))

(s/def ::datetime (dev-with-gen
                   #(or
                     ;; mysql select returns joda-time
                     (instance? org.joda.time.DateTime %)
                     (instance? java.util.Date %))
                   #(s/gen #{#inst "2000-01-01T00:00:00.000-00:00"})))

(s/def ::id ::uuid-str)
(s/def ::entity-str (s/and string? #(<= (count %) 36)))
(s/def ::entity keyword?)
(s/def ::k keyword?)
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

;; ## ::data is the data stored in the hashdb
;;
;; We use two different specs, one for development (or actually test-case
;; data generation), and one for production.
;;
;; My first attempt was like this, however, then I cannot force the
;; data for the keys :s1 :s2 :s3 :s4 to be strings
;;
;;     (s/def ::key
;;       (dev-spec-or :normal keyword? :dev #{:a :b :c :d :e}))
;;     (s/def ::data
;;       (s/map-of ::key any?))
;;
;; The correct way is to use s/merge and s/keys, which merges maps.
;;

(if-not (:dev env)
  (s/def ::data
    (s/map-of keyword? any?))
  (do
    ;; during testing, there are the keys that are indexed
    (def possible-keys #{:s1 :s2 :s3 :s4})
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
      ;; ::indexed-data should be first, it created samples without any :s1 :s2 :s3 :s4
      (s/merge ::indexed-data (s/map-of ::short-keyword any?)))))

;; `::changes` are the set of key-value pairs that are used to update the stored data
;; for changes, it is okay to set nil, which means remove key
(s/def ::changes
  (s/map-of keyword? (s/nilable some?)))

;; The complete stored data.
;;
;; TODO: where is ::data?
(s/def ::create-latest
  (s/keys :req-un [::id ::entity ::updated ::version]
          ;; ::tenant optional since optional for create!
          :opt-un [::tenant]))

(s/def ::stored-latest
  (s/keys :req-un [::id ::tenant ::entity ::updated ::version]
          :opt-un []))

;; The history record.
(s/def ::stored-history
  (s/keys :req-un [::id ::tenant ::entity ::deleted ::before ::after
                   ::updated ::version ::parent]
          :opt-un [::is_merge ::userid ::sessionid ::comment]))

;; The history record without the data.
(s/def ::stored-history-short
  (s/keys :req-un [::id ::tenant ::entity ::deleted ::updated ::version ::parent]
          :opt-un [::is_merge ::userid ::sessionid ::comment]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # SPECs for indexes

;; Indexes can either be string or integer.
(s/def ::idx-type #{:string :long})

;; The list of all indexed keys, and their type.
(s/def ::idx-info
  (s/tuple (s/map-of keyword? ::idx-type)
           (s/coll-of keyword? :into #{})))

(s/fdef indexes
        :args (s/cat :m ::data)
        :ret  ::idx-info)

;; TODO: implement a function that returns this list.
;;
;; TODO: For now, it is hard-coded. Only works for test-cases
(defn indexes
  "Depending on the entity and other fields in `m`, return the indexes.
   Only non-nil values will be indexed."
  ;; TODO: depends on (get-tenant)
  [m]
  (let [raw {:s1 :string, :s2 :string, :s3 :string, :s4 :string,
             :i1 :long, :i2 :long}]
    [raw (into #{} (map key raw))]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Update indexes

;; Algorithm #1 for update-indexes!
;;
;; * Keep only fields in before and after that have indexes
;; * For the difference, 3 options:
;;  * nil or non-existent in before, exists in after => insert
;;  * exists in before, nil in after => delete
;;  * exists in before, exists in after => update
;;
;; Optimization: if I want to use multi-insert, it is best if we loop per type,
;; i.e. first for :string, then for :int

(s/fdef keep-difference-by-type
        :args (s/cat :changes ::changes :idx-info ::idx-info)
        :ret  ::data)

(defn keep-difference-by-type
  "Only keep the changes keys that have indexes.
   Group these by type, e.g. :string :int..."
  [changes [idx-types idx-keys]]
  (if-not changes {}
          (let [changes-relevant (select-keys changes idx-keys)
                changes-grouped  (mapify-values
                                  (group-by #(idx-types (key %))
                                            changes-relevant))]
            changes-grouped)))


(defn- update-indexes-one-type-create!
  "Create index entries for new record `id`."
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
  "Update index entries for already existing record `id`."
  [conn before changes typ id entity]
  (let [keys-before (set (keys before))
        keys-changes0 (set (keys changes))
        disappeared(clojure.set/difference keys-before keys-changes0)
        _ (assert (empty? disappeared)
                  (str "Illegal update, the following keys needs to be part of changes" disappeared))
        tmp (group-by #(nil? (second %)) changes)
        map-changes (clojure.core/get tmp false)
        map-deleted (clojure.core/get tmp true)
        key-changes (keys map-changes)
        should-be-updated (clojure.set/intersection
                           (set key-changes) keys-before)
        should-be-created (clojure.set/difference
                           (set (keys map-changes)) keys-before)]
    (when (= :string typ)
      ;; Sort to minimize risk of deadlock
      ;; MySQLTransactionRollbackException Deadlock found when trying to get lock;
      ;; try restarting transaction:
      ;; com.mysql.cj.jdbc.exceptions.SQLError.createSQLException (SQLError.java:539)
      ;;
      ;; Todo: If it still happends, I have to sort delete+create+update according to :k
      (doseq [[k v] (sort map-deleted)]
        ;; v = nil means deleted
        (assert (nil? v))
        (let [res (cmd/delete-single-string-index!
                   conn {:id id, :entity (str-edn entity), :k (str-edn k)})]
          (assert (= 1 res))))

      (doseq [k (sort should-be-created)]
        ;; no point in adding nil:s to index, so should never happen,
        ;; even if has nil value
        (assert (k changes))
        (let [res (cmd/create-string-index!
                   conn {:id id, :entity (str-edn entity), :k (str-edn k)
                         :index_data (str-index (k changes))})]
          (assert (= 1 res))))
      (doseq [k (sort should-be-updated)]
        ;; update to nil, same as deleting (but this case should not happen
        ;; since then changes will not fulfil ::data)
        (assert (k changes))
        (cmd/update-string-index!
         conn {:id id, :entity (str-edn entity), :k (str-edn k)
               :index_data (str-index (k changes))})))
    nil))


;; For delete, we can just delete all index entries at once
;; so actually unnecessary to see which exact keywords are indexed,
;; just which type of indexes there are. No point deleting int-indexes
;; there are none.
(defn- update-indexes-one-type-delete!
  "Delete al index entries for record `id`."
  [conn changes typ id]
  (assert (empty? changes))
  (if (= :string typ)
    (cmd/delete-string-index! conn {:id id}))
  nil)


;; Are we creating a new record, updating an existing, or deleting an old one?
(s/def ::sql-op #{:create :update :delete})


(s/fdef update-indexes-one-type!
        :args (s/cat :conn any? :sql-op ::sql-op :typ ::idx-type :m ::data
                     :before (s/nilable ::data) :changes ::changes)
        :ret nil?)

(defn update-indexes-one-type!
  "Update the index entries for record `m`.
   sql-op tells if `m` is a new record, an existing,
   or if the record `m` will be removed."
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
                     :before ::data :changes ::changes)
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
      (let [before0 (typ before-relevant)
            changes0 (typ changes-relevant)]
        (if (or before0 changes0)
          ;; changes0 required to be map according to spec
          (update-indexes-one-type! conn sql-op typ m before0 (or changes0 {})))))))



(s/fdef delete-indexes-without-data!
        :args (s/cat :conn any? :id ::id)
        :ret nil?)

(defn delete-indexes-without-data!
  "Delete all index entries for record `id`.
   Can be inefficient, since we have to assume there are both
   string and integer indexed keys."
  [conn id]
  (doseq [typ [:string :long]]
    (update-indexes-one-type-delete! conn {} typ id)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Create: Insert map `m` into db.

(s/fdef create!
        :args (s/cat :m ::data)
        :ret ::create-latest)

(defn create!
  "Insert map `m` into db.
   If `:id` is in `m`, use it, otherwise create one.
   Return the map incl the potentially created id."
  [m]
  (let [id0        (or (:id m) (uuid))
        tenant     (or (:tenant m) (get-tenant))
        tenant-str (tenant->str tenant)
        _          (verify-tenant-write tenant)
        entity     (or (:entity m) :unknown)
        entity-str (str-edn entity)
        now0       (now)
        version    1
        m          (into m {:id      id0     :tenant tenant :updated now0
                            :version version :entity entity})
        data       (str-edn m)]
    (sql/with-db-transaction [conn *db*]
      (cmd/create-latest! conn
                          {:id      id0        :tenant tenant-str
                           :entity  entity-str :data   data
                           :updated now0       :parent 0 :version version})
      (cmd/create-history! conn
                           {:id      id0,           :tenant    tenant-str
                            :entity  entity-str,    :deleted   0,
                            :before  empty-map-edn, :after     data,
                            :updated now0,          :version   version,
                            :parent  0,             :is_merge  0,
                            :userid  nil,           :sessionid nil,
                            :comment nil})
      (update-indexes! conn :create m {} m)
      m)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Verify duplicated data in SQL-table row and inside record `m` are identical.

;; JDK8 has a new time library: https://github.com/dm3/clojure.java-time
(defn max-1-second-diff
  "mySQL-time (joda time) and java time seems to be slightly different.
   If they are at most 1s apart, assume equal."
  [t1 t2]
  (let [diff (t/in-millis
              (if (t/before? t1 t2)(t/interval t1 t2)(t/interval t2 t1)))]
    ;; (println diff)
    ;; seem that the diff can be up to 400ms
    (< diff 1000)))


(defn verify-row-in-sync
  "Make sure db `row` is in sync with `m` from the :data column."
  [row m]
  (assert (and (= (:id row)(:id m))
               (= (:version row)(:version m))
               (= (:tenant row)(tenant->str (:tenant m)))
               (= (:entity row)(str-edn (:entity m)))
               ;; https://stackoverflow.com/questions/15333320/how-to-convert-joda-time-datetime-to-java-util-date-and-vice-versa
               (let [m-updated (org.joda.time.DateTime. (:updated m))
                     row-updated (:updated row)]
                 (max-1-second-diff m-updated row-updated)))
          "id, tenant, entity, updated, and/or version and data columns in table latest are not in sync.")
  m)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Read record helpers from db

(defn- extract-row
  "Deserialize the map, and verify that in sync with rest of SQL-row."
  [row]
  (let [m (clojure.edn/read-string (:data row))]
    (when m (verify-row-in-sync row m))
    m))


(defn- try-get-internal
  "Return map at `id`, null if not found."
  [id row]
  (let [row (cmd/get-latest {:id id})
        m   (extract-row row)]
    (when row (assert (and (= id (:id row))
                           (verify-tenant-read (:tenant m)))
                      "id and/or tenant, version and data columns in table latest are not in sync."))
    m))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Read single record from db using `id`

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
    (if m m (throw (ex-info (str "Row with id '" id "' missing!")
                            {:id id})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Read many records from database

(s/fdef select-all
        :args (s/cat)
        :ret (s/* ::stored-latest))

(defn select-all
  "Return all rows."
  []
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest {:tenant (tenant->str (get-tenant))})))


(s/fdef select-all-by-entity
        :args (s/cat :entity ::entity)
        :ret (s/* ::stored-latest))

(defn select-all-by-entity
  "Return all rows for a given `entity`."
  [entity]
  (map #(try-get-internal (:id %) %)
       (cmd/select-all-latest-by-entity {:tenant (tenant->str (get-tenant))
                                         :entity (str-edn entity)})))


(s/fdef select-all-nil-entity
        :args (s/cat)
        :ret (s/* ::stored-latest))

(defn select-all-nil-entity
  "Return all rows for unknown `entity`."
  []
  (select-all-nil-entity :unknown))

;; (cmd/select-by-string-index {:entity ":unknown" :k ":s1" :index_data "lena"})

(s/fdef select-by-string
        :args (s/cat :entity ::entity :k ::k :search string?)
        :ret  (s/* ::stored-latest))

(defn select-by-string
  "Return all rows of type `entity` where `k` is exactly the string `search`."
  [entity k search]
  (let [res (cmd/select-by-string-index
             {:tenant (tenant->str (get-tenant))
              :entity (str-edn entity) :k (str-edn k)
              :index_data (str-index search)})]
    (map extract-row res)))


(s/fdef select-by-string-global
        :args (s/cat :entity ::entity :k ::k :search string?)
        :ret  (s/* ::stored-latest))

(defn select-by-string-global
  "Return all rows of type `entity` where `k` is exactly
   the string `search` regardless of tenant."
  [entity k search]
  (assert (= :global (get-tenant-raw))
          (str "For global search, you need to set tenant to :global, not '"
               (get-tenant-raw) "'"))
  (let [res (cmd/select-by-string-index-global
             {:entity (str-edn entity) :k (str-edn k)
              :index_data (str-index search)})]
    (map extract-row res)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Update single record `m` in db

(s/fdef update!
        :args (s/cat :m ::stored-latest :changes ::changes)
        :ret ::stored-latest)

(defn update!
  "Map `m` is the one currently stored in the db.
   The map `changes` contains the fields that should be updated.
   Return the new map, or throw exception if update fails."
  [m changes]
  (assert (nil? (:id changes)) "Changing :id is not allowed!")
  (assert (nil? (:tenant changes)) "Changing :tenant is not allowed!")
  (assert (nil? (:entity changes)) "Changing :entity is not allowed!")
  (assert (and (some? (:version m))(>= (:version m) 1)) "Version 0 should be create:d!")
  (let [id         (nn (:id m))
        tenant     (nn (:tenant m))
        tenant-str (tenant->str tenant)
        _          (verify-tenant-write tenant)
        parent     (nn (:version m))
        ;; for the non-acid update, (inc parent) will not do,
        ;; maybe I need to using timestamp again
        version    (inc parent)
        updated    (now)
        before     (select-keys m (keys changes))
        data       (into (into m changes) {:updated updated :version version})
        data-str   (str-edn data)
        entity-str (str-edn (:entity m))]
    (sql/with-db-transaction [conn *db*]
      (let [affected (cmd/update-latest!
                      conn {:id      id      :tenant  tenant-str
                            :parent  parent  :updated updated
                            :version version :data    data-str})]
        (cond (= 0 affected)
              (throw (ex-info (str "Row '" id "' has been updated since read '" parent "'")
                              {:id id :updated parent}))
              (> affected 1)
              (throw (ex-info (str "Row '" id "' existed several times in db.")
                              {:id id :updated parent})))

        ;; why not in a transaction? since insert, it cannot fail.
        ;; and for non-acid-update, the history is the long-term truth,
        ;; not the latest entry
        (cmd/create-history!
         conn
         {:id       id,               :tenant    tenant-str
          :entity   entity-str,       :deleted   0,
          :before   (str-edn before), :after     (str-edn changes),
          :updated  updated,          :version   version, :parent  parent,
          :is_merge 0,
          :userid   nil,              :sessionid nil,     :comment nil})
        (update-indexes! conn :update m before changes))
      data)))



(s/fdef update-diff!
        :args (s/cat :m ::stored-latest :changes ::changes)
        :ret ::stored-latest)

(defn update-diff!
  "Find the differences made and then update db."
  [m-old m-new]
  (let [changes (map-difference m-new m-old)]
    (update! m-old changes)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Delete single record `m` from database db

(s/fdef delete!
        :args (s/cat :m ::data)
        :ret nil?)

(defn delete!
  "Delete the row for `m`.
   We do not care if anyone has updated or deleted the row just before.
   Will leave a perfect history, if the `m` you send in the is the latest."
  [m]
  (assert (and (:id m) (:version m)) ":id & :version is minimum for delete.")
  (sql/with-db-transaction [conn *db*]
    (let [entity-str (str-edn (:entity m))
          tenant     (nn (:tenant m))
          _          (verify-tenant-write tenant)
          tenant-str (tenant->str tenant)
          affected   (cmd/delete-latest! conn {:id (:id m) :tenant tenant-str})
          _          (assert (= affected 1) "No row deleted by delete")]
      (cmd/create-history!
       conn
       {:id      (:id m),      :tenant    tenant-str
        :entity  entity-str,   :deleted   1,
        :before  (str-edn m),  :after     empty-map-edn,
        :updated (now),        :version   (inc (:version m)),
        :parent  (:version m), :is_merge  0,
        :userid  nil,          :sessionid nil, :comment nil})
      (update-indexes! conn :delete m m {})
      nil)))


(s/fdef delete-by-id-with-minimum-history!
        :args (s/cat :id ::id)
        :ret nil?)

(defn delete-by-id-with-minimum-history!
  "Delete the row `id`.
   The last history entry will not be optimal.
   The existing record will not be read before.
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (sql/with-db-transaction [conn *db*]
    (let [tenant     (get-tenant)
          tenant-str (tenant->str tenant)
          affected   (cmd/delete-latest! conn {:id id :tenant tenant-str})
          _          (assert (= affected 1) "No row deleted by delete")]
      (cmd/create-history!
       conn
       {:id      id,                 :tenant    tenant-str
        :entity  (str-edn :unknown), :deleted   1,
        :before  empty-map-edn,      :after     empty-map-edn,
        :updated (now),              :version   2000000001,
        :parent  2000000000,         :is_merge  0,
        :userid  nil,                :sessionid nil,
        :comment nil})
      (delete-indexes-without-data! conn id)
      nil)))


(s/fdef delete-by-id!
        :args (s/cat :id ::uuid-str)
        :ret nil?)

(defn delete-by-id!
  "Delete the row `id`.
   It will make sure the history is perfect by reading the record first (= 2 SQL operations).
   We do not care if anyone has updated or deleted the row just before."
  [id]
  (if-let [m (try-get id)]
    (sql/with-db-transaction [conn *db*]
      (let [version    (:version m)
            tenant     (nn (:tenant m))
            _          (verify-tenant-write tenant)
            tenant-str (tenant->str tenant)
            affected   (cmd/delete-latest! conn {:id (:id m) :tenant tenant-str})
            _          (assert (= affected 1) "No row deleted by delete")]
        (cmd/create-history!
         conn
         {:id      (:id m),               :tenant    tenant-str
          :entity  (str-edn (:entity m)), :deleted   1,
          :before  (str-edn m),           :after     empty-map-edn,
          :updated (now),                 :version   (inc version),
          :parent  version,               :is_merge  0,
          :userid  nil,                   :sessionid nil,
          :comment nil})
        (delete-indexes-without-data! conn id)
        nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; # Read history records from database
;;
;; The history records describe all operations applied to a single record `id`.

(defn- deserialize-history
  "Deserialize the :before and :after keys of `m`."
  [m]
  (into m {:after  (clojure.edn/read-string (:after m))
           :before (clojure.edn/read-string (:before m))
           :entity (clojure.edn/read-string (:entity m))
           :tenant (str->tenant (:tenant m))}))


(s/fdef history
        :args (s/cat :id ::uuid-str)
        :ret  (s/* ::stored-history))

(defn history
  "Get the complete history for `id`."
  [id]
  (map deserialize-history
       (cmd/select-history {:id id
                            :tenant (tenant->str (get-tenant))})))


(s/fdef history-by-entity
        :args (s/cat :entity ::entity)
        :ret  (s/* ::stored-history))

(defn history-by-entity
  "Get the complete history for all entities of `entity`."
  [entity]
  (map deserialize-history (cmd/select-history-by-entity
                            {:tenant (tenant->str (get-tenant))
                             :entity (str-edn entity)})))


(s/fdef history-nil-entity
        :args (s/cat)
        :ret  (s/* ::stored-history))

(defn history-nil-entity
  "Get the complete history for all :unknown entities."
  []
  (history-by-entity :unknown))


;; `history-short` operation is most likely much faster for long histories since:
;;
;; * no deserialization of :data
;; * InnoDB puts only 767 bytes of a TEXT or BLOB inline, the rest goes into some other block.
;;   This is a compromise that sometimes helps, sometimes hurts performance.
(s/fdef history-short
        :args (s/cat :id ::id)
        :ret  (s/* ::stored-history-short))

(defn history-short
  "Get the complete history for `id`, but do not read :before and :after.
   Used to present who changed anything for this object."
  [id]
  (map deserialize-history
       (cmd/select-history-short {:id id
                                  :tenant (tenant->str (get-tenant))})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; # Validation of database
;;;
;;; * Make sure record and SQL row are in sync (See above). (DONE)
;;; * Ensure latest and index are in sync. (DONE)
;;; * Reconstruct latest from history, to make sure history is correct. (NOT DONE)

(s/fdef verify-stored-data
        :args (s/cat :id ::id)
        :ret  boolean?)

(defn verify-stored-data
  "Make sure the `m` with `id` is correctly and completely stored.
   If not, print out warning and return false."
  [id]
  (let [m           (try-get id)
        idxs        (cmd/select-string-index {:id id})
        idxs-as-map (into {}
                          (map (fn [idx] [(clojure.edn/read-string (:k idx))
                                          (:index_data idx)])
                               idxs))]
    (if m
      (let [idx-info    (indexes m)
            idx-values  (select-keys m (second idx-info))
            diff        (keep-difference-by-type m idx-info)]
        (and (or (zero? (count idxs))
                 (= (:entity m)
                    (clojure.edn/read-string (:entity (first idxs)))))
             (= (count idxs) (count idxs-as-map))
             (= (count idxs) (count idx-values))
             (empty? (map-difference idxs-as-map idx-values))))
      (zero? (count idxs)))))


(defn verify-these
  "Verify these `ids`."
  [ids]
  (doseq [id ids]
    (when-not (verify-stored-data id)
      (println (str "'" id "' not ok")))))


(defn verify-all-stored-data
  "Read all database and verify it."
  []
  (let [ids (map :id (cmd/select-all-latest))]
    (verify-these ids)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(orchestra.spec.test/instrument)
