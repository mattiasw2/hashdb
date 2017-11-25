(ns user
  (:require [luminus-migrations.core :as migrations]
            [hashdb.config :refer [env]]
            ;; Added hashdb.db.* so that (start) works
            hashdb.db.core
            hashdb.db.commands
            [mount.core :as mount]
            hashdb.core))


(defn migrate []
  (migrations/migrate ["migrate"] (select-keys env [:database-url])))

(defn rollback []
  (migrations/migrate ["rollback"] (select-keys env [:database-url])))

;; How to create migration files from repl
;; (luminus-migrations.core/create "create-string-index" {})


(defn start []
  (mount/start))

(defn stop []
  (mount/stop))
