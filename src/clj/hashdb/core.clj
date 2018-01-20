(ns hashdb.core
  (:require
   [luminus-migrations.core :as migrations]
   [hashdb.config :refer [env]]
   [clojure.tools.cli :refer [parse-opts]]
   [clojure.tools.logging :as log]
   [mount.core :as mount])
  (:gen-class))

(def cli-options
  [["-p" "--port PORT" "Port number"
    :parse-fn #(Integer/parseInt %)]])

(defn stop-app []
  (doseq [component (:stopped (mount/stop))]
    (log/info component "stopped"))
  (shutdown-agents))

(defn start-app [args]
  (doseq [component (-> args
                        (parse-opts cli-options)
                        mount/start-with-args
                        :started)]
    ;; for nanobox experiments, create db tables unless already done
    (migrations/migrate ["migrate"] (select-keys env [:database-url :migration-dir]))
    (log/info component "started"))
  (.addShutdownHook (Runtime/getRuntime) (Thread. stop-app)))

(defn -main [& args]
  (cond
    (some #{"init"} args)
    (do
      (mount/start #'hashdb.config/env)
      (migrations/init (select-keys env [:database-url :init-script :migration-dir]))
      #_(System/exit 0))
    (some #{"migrate" "rollback"} args)
    (do
      (mount/start #'hashdb.config/env)
      (migrations/migrate args (select-keys env [:database-url :migration-dir]))
      #_(System/exit 0))
    :else
    (start-app args)))
