(ns hashdb.config
  (:require [cprop.core :refer [load-config]]
            [cprop.source :as source]
            [mount.core :refer [args defstate]]))

(defn nanobox-env
  [m]
  (let [connstr (str "mysql://" (:data-mysql-host m)  ":3306/gonano?user=root&password=" (:data-mysql-root-pass m) "&autoReconnect=true&useSSL=false")
        res (if (:data-mysql-host m)
              ;; "mysql://172.20.0.4:3306/gonano?user=root&password=QoCsfDL6CL&autoReconnect=true&useSSL=false",
              (assoc m :database-url connstr)
              m)]
    (println (:database-url res))
    res))


(defstate env :start (-> (load-config
                          :merge
                          [(args)
                           (source/from-system-props)
                           (source/from-env)])
                         nanobox-env
                         (assoc :migration-dir "hashdb_migrations")))
