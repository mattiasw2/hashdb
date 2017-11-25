(ns hashdb.db.core
  (:require
    [clj-time.jdbc]
    [clojure.java.jdbc :as jdbc]
    [clojure.java.jdbc :as sql]
    [conman.core :as conman]
    [hashdb.config :refer [env]]
    [mount.core :refer [defstate]])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))

(defstate ^:dynamic *db*
           :start (conman/connect! {:jdbc-url (env :database-url)})
           :stop (conman/disconnect! *db*))

;; do not use the default name queries.sql if there are
;; multiple projects involved.
(conman/bind-connection *db* "sql/hashdb.sql")
