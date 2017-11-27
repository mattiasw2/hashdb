(ns user
  (:require [luminus-migrations.core :as migrations]
            [hashdb.config :refer [env]]
            ;; Added hashdb.db.* so that (start) works
            hashdb.db.core
            hashdb.db.commands
            [mount.core :as mount]
            hashdb.core))


(defn get-config
  []
  (into {:migration-dir "hashdb_migrations"}
        (select-keys env [:database-url])))


(defn migrate []
  (migrations/migrate ["migrate"] (get-config)))

(defn rollback []
  (migrations/migrate ["rollback"] (get-config)))

;; How to create migration files from repl
;; (luminus-migrations.core/create "create-string-index" {})


(defn start []
  (mount/start))

(defn stop []
  (mount/stop))
