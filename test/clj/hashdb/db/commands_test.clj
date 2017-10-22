(ns hashdb.db.commands-test
  (:require
   [clojure.test :refer :all]
   [hashdb.db.command :refer :all]
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


;; (hashdb.db.commands/create {:då "foo"})
;; (hashdb.db.commands/update (hashdb.db.commands/get "6df4b724-7519-4296-b5b9-021aaaa58033") {:bar "foo"})
;; (hashdb.db.commands/delete (hashdb.db.commands/get "6df4b724-7519-4296-b5b9-021aaaa58033"))
;; (clojure.pprint/pprint (hashdb.db.commands/history "6df4b724-7519-4296-b5b9-021aaaa58033"))
