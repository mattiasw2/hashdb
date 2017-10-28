(ns user
  (:require [luminus-migrations.core :as migrations]
            [hashdb.config :refer [env]]
            ;; Added hashdb.db.* so that (start) works
            hashdb.db.core
            hashdb.db.commands
            [mount.core :as mount]
            [hashdb.figwheel :refer [start-fw stop-fw cljs]]
            hashdb.core))

(defn start []
  (mount/start-without #'hashdb.core/repl-server))

(defn stop []
  (mount/stop-except #'hashdb.core/repl-server))

(defn restart []
  (stop)
  (start))

(defn migrate []
  (migrations/migrate ["migrate"] (select-keys env [:database-url])))

(defn rollback []
  (migrations/migrate ["rollback"] (select-keys env [:database-url])))

;; How to create migration files from repl
;; (luminus-migrations.core/create "create-string-index" {})
