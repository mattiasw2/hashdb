(ns hashdb.db.commands-test
  (:require
   [clojure.test :refer :all]
   [hashdb.db.commands :refer :all]
   [clj-time.jdbc]
   [clojure.java.jdbc :as jdbc]
   [clojure.java.jdbc :as sql]
   [hashdb.config :refer [env]]
   [hashdb.db.core :as cmd]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen]
   [mw.std :refer :all]
   [clojure.spec.test.alpha :as stest])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))

(s/def ::small-map (s/and map? #(< (count %) 20)))
(s/def ::large-map (s/and map? #(> (count %) 200)))

(defn play-with-spec-test
  []
  ;; (s/conform ::uuid-str "0000000000000000000000000000000000000")
  (stest/check `create)
  #_(stest/check `delete-by-id-with-minimum-history))

;; below timing result DISTORTED by slow generation of sample data
;; (timed "exercise" (def m1 (first (last (s/exercise map? 1000)))))
;; "Timed: exercise : 34237.450385 msecs"

;; with compression

;; (timed "exercise" (def m2s (mapv first (s/exercise :hashdb.db.commands/data 1000))))
;; "Timed: exercise : 16754.472192 msecs"
;; #'hashdb.db.commands-test/m2s
;; hashdb.db.commands-test> (timed "" (doseq [m m2s] (create m)))
;; "Timed:  doseq: 7299.823052 msecs"
;; nil
;; hashdb.db.commands-test>

;; (timed "" (def saved-m2s (mapv create m2s)))

;; "Timed:  : 7086.722663 msecs"
;; #'hashdb.db.commands-test/saved-m2s
;; hashdb.db.commands-test>
;; (first saved-m2s)
;; {:s nil, :* nil, :+ 1.0, :id "30a7b320-fe4d-4216-9ba5-c1db4e9c799c", :updated #inst "2017-10-25T11:55:56.783-00:00", :version 1}

;; **************************************************************** update
;; update takes same sort of time, i.e 100+ per second
;; hashdb.db.commands-test> (timed "" (def saved-m3s (mapv #(update % {:hej "mattias"}) saved-m2s)))
;; "Timed:  : 6265.730056 msecs"

;; **************************************************************** delete
;; (timed "" (def saved-m4s (mapv #(delete %) saved-m3s)))
;; "Timed:  : 6083.791703 msecs"

;; the above performance is for 1 thread, i.e. depends on latency
;; (timed "exercise" (def m2s (mapv first (s/exercise :hashdb.db.commands/data 1000))))
;; "Timed: exercise : 15841.978657 msecs"
;; #'hashdb.db.commands-test/m2s
;; hashdb.db.commands-test> (timed "" (doseq [m m2s] (create m)))
;; "Timed:  doseq: 8725.000227 msecs"
;; nil
;; hashdb.db.commands-test> (timed "exercise" (def n2s (mapv first (s/exercise :hashdb.db.commands/data 1000))))
;; "Timed: exercise : 15583.56395 msecs"
;; #'hashdb.db.commands-test/n2s
;; hashdb.db.commands-test> (future (timed "" (doseq [m m2s] (create m))))
;; #future[{:status :pending, :val nil} 0x36ea0243]
;; hashdb.db.commands-test> (future (timed "" (doseq [m n2s] (create m))))
;; #future[{:status :pending, :val nil} 0x3204baed]
;; hashdb.db.commands-test> "Timed:  doseq: 7902.750535 msecs"
;; "Timed:  doseq: 7708.286371 msecs"



;; (doseq [m (s/exercise map? 1000)] (create (first m)))
;; (doseq [m (s/exercise ::small-map 1000)] (create (first m)))
;; (doseq [m (s/exercise ::large-map 1000)] (create (first m)))
;;
;; big stuff is slow to store, make two tests: small-maps and big-maps

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
        m2  (hashdb.db.commands/create {:bar "rolf" :uppsala 10})
        id2 (:id m2)
        m3  (hashdb.db.commands/create {:gunnar "mattias"})
        id3 (:id m3)]
    (hashdb.db.commands/update (hashdb.db.commands/get id1) {:bar "foo"})
    (let [m2-1 (hashdb.db.commands/get id2)]
      (hashdb.db.commands/update-diff m2-1 (into m2-1 {:uppsala 20 :sundsvall 30})))
    (hashdb.db.commands/delete-by-id id2)
    (hashdb.db.commands/delete (hashdb.db.commands/get id1))
    (hashdb.db.commands/update (hashdb.db.commands/get id3) {:bar "foo", :gunnar "lena"})
    (hashdb.db.commands/delete-by-id-with-minimum-history id3)
    (println "m1")
    (clojure.pprint/pprint (hashdb.db.commands/history id1))
    (println "m2")
    (clojure.pprint/pprint (hashdb.db.commands/history id2))
    (println "m2-short")
    (clojure.pprint/pprint (hashdb.db.commands/history-short id2))
    (println "m3")
    (clojure.pprint/pprint (hashdb.db.commands/history-nil-entity))))


;; bugs:

;; 1. (hashdb.db.commands/history-by-entity nil) not working, returns () => fixed
