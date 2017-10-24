(ns hashdb.db.commands-test
  (:require
   [clojure.test :refer :all]
   [hashdb.db.commands :refer :all]
   [clj-time.jdbc]
   [clojure.java.jdbc :as jdbc]
   [clojure.java.jdbc :as sql]
   [hashdb.config :refer [env]]
   [hashdb.db.core :as cmd])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))


;; (deftest test-app
;;   (testing "main route"
;;     (let [response ((app) (request :get "/"))]
;;       (is (= 200 (:status response)))))

;;   (testing "not-found route"
;;     (let [response ((app) (request :get "/invalid"))]
;;       (is (= 404 (:status response))))))


(defn test-all-commands
  []
  (let [m1  (hashdb.db.commands/create {:då "foo"})
        id1 (:id m1)
        m2  (hashdb.db.commands/create {:bar "rolf"})
        id2 (:id m2)
        m3  (hashdb.db.commands/create {:gunnar "mattias"})
        id3 (:id m3)]
    (hashdb.db.commands/update (hashdb.db.commands/get id1) {:bar "foo"})
    (hashdb.db.commands/delete-by-id id2)
    (hashdb.db.commands/delete (hashdb.db.commands/get id1))
    (hashdb.db.commands/update (hashdb.db.commands/get id3) {:bar "foo", :gunnar "lena"})
    (hashdb.db.commands/delete-by-id-with-minimum-history id3)
    (println "m1")
    (clojure.pprint/pprint (hashdb.db.commands/history id1))
    (println "m2")
    (clojure.pprint/pprint (hashdb.db.commands/history-short id2))
    (println "m3")
    (clojure.pprint/pprint (hashdb.db.commands/history-nil-entity))))


;; bugs:

;; 1. (hashdb.db.commands/history-by-entity nil) not working, returns () => fixed
