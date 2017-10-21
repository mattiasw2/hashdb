(ns hashdb.db.commands
  (:require
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


(defn create
  "Insert map `m`.
   If `:id` is in `m`, use it, otherwise create one.
   Return the map incl the potentially created id."
  [m]
  (let [id0 (or (:id m) (uuid))
        now0 (now)
        version 1
        m (into m {:id id0 :updated now0 :version version})]
    (cmd/create-latest! {:id id0 :data (pr-str m) :updated (now) :parent 0 :version version})
    m))

(defn verify-row-in-sync
  [m]
  (assert (and (= (:id m)(:id (:data m)))
               (= (:version m)(:version (:data m))))
          "id and/or version and data columns in table latest are not in sync.")
  m)

(defn try-get
  "Return map at `id`, null if not found."
  [id]
  (let [m (cmd/get-latest {:id id})
        res (clojure.edn/read-string (:data m))]
    (when m (assert (and (= id (:id m)))
                    "id and/or version and data columns in table latest are not in sync."))
    (when res (verify-row-in-sync res))
    res))

(defn get
  "Return map at `id`, exception if not found."
  [id]
  (let [m (try-get id)]
    (if m m (throw (ex-info (str "Record with id " id " missing!")
                            {:id id})))))

(defn select-all
  "Return all maps."
  []
  (map #(clojure.edn/read-string (:data (verify-row-in-sync %)))
       (cmd/select-all-latest)))

(defn update
  "Map `m` is the one currently stored in the db.
   The map `changes` contains the fields that should be updated.
   Return the new map, or throw exception if update fails."
  [m changes]
  (let [id (nn (:id m))
        parent (nn (:version m))
        version (inc parent)
        updated (now)
        data (pr-str (into (into m changes) {:updated updated :version version}))
        affected (cmd/update-latest! {:id id :parent parent :updated updated :version version :data data})]
    (cond (= 1 affected) data
          (= 0 affected) (throw (ex-info (str "Record " id " has been updated since read " parent)
                                         {:id id :updated parent}))
          (> affected 1) (throw (ex-info (str "Record " id " existed several times in db.")
                                         {:id id :updated parent})))))
