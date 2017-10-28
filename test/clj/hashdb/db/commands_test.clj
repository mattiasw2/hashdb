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
  (stest/check `create!)
  #_(stest/check `delete-by-id-with-minimum-history))

;; below timing result DISTORTED by slow generation of sample data
;; (timed "exercise" (def m1 (first (last (s/exercise map? 1000)))))
;; "Timed: exercise : 34237.450385 msecs"

;; with compression

;; (timed "exercise" (def m2s (mapv first (s/exercise :hashdb.db.commands/data 1000))))

;; hashdb.db.commands-test> (timed "create!" (def saved-m2s (mapv create! m2s)))
;; "Timed: create! : 16609.213303 msecs"
;; #'hashdb.db.commands-test/saved-m2s
;; hashdb.db.commands-test> (first saved-m2s)
;; {:y nil, :L [], :* (), :F [], :_ [], :updated #inst "2017-10-28T08:53:27.446-00:00", :P (), :D [], :B nil, :s4 "", :k nil, :g nil, :+ [], :h nil, :id "43eeb8f7-70a7-4223-a60b-ee08db9de13a", :d (), :i2 0, :version 1, :! nil, :i1 0, :s1 "", :U (), :s3 "", :u [], :s2 ""}
;; hashdb.db.commands-test> (timed "update" (def saved-m3s (mapv #(update % {:hej "mattias"}) saved-m2s)))
;; ArityException Wrong number of args (2) passed to: core/update  clojure.lang.AFn.throwArity (AFn.java:429)
;; hashdb.db.commands-test> (timed "update" (def saved-m3s (mapv #(update! % {:hej "mattias"}) saved-m2s)))
;; "Timed: update : 11139.300576 msecs"
;; #'hashdb.db.commands-test/saved-m3s
;; hashdb.db.commands-test> (timed "delete" (def saved-m4s (mapv #(delete! %) saved-m3s)))
;; "Timed: delete : 10234.706684 msecs"
;; #'hashdb.db.commands-test/saved-m4s
;; hashdb.db.commands-test>


;; the above performance is for 1 thread, i.e. depends on latency
;; (timed "exercise" (def m2s (mapv first (s/exercise :hashdb.db.commands/data 1000))))
;; "Timed: exercise : 15841.978657 msecs"
;; #'hashdb.db.commands-test/m2s
;; hashdb.db.commands-test> (timed "" (doseq [m m2s] (create! m)))
;; "Timed:  doseq: 8725.000227 msecs"
;; nil
;; hashdb.db.commands-test> (timed "exercise" (def n2s (mapv first (s/exercise :hashdb.db.commands/data 1000))))
;; "Timed: exercise : 15583.56395 msecs"
;; #'hashdb.db.commands-test/n2s
;; hashdb.db.commands-test> (future (timed "" (doseq [m m2s] (create! m))))
;; #future[{:status :pending, :val nil} 0x36ea0243]
;; hashdb.db.commands-test> (future (timed "" (doseq [m n2s] (create! m))))
;; #future[{:status :pending, :val nil} 0x3204baed]
;; hashdb.db.commands-test> "Timed:  doseq: 7902.750535 msecs"
;; "Timed:  doseq: 7708.286371 msecs"



;; (doseq [m (s/exercise map? 1000)] (create! (first m)))
;; (doseq [m (s/exercise ::small-map 1000)] (create! (first m)))
;; (doseq [m (s/exercise ::large-map 1000)] (create! (first m)))
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
  (let [m1  (hashdb.db.commands/create! {:d� "foo"})
        id1 (:id m1)
        m2  (hashdb.db.commands/create! {:bar "rolf" :uppsala 10})
        id2 (:id m2)
        m3  (hashdb.db.commands/create! {:gunnar "mattias"})
        id3 (:id m3)]
    (hashdb.db.commands/update! (hashdb.db.commands/get id1) {:bar "foo"})
    (let [m2-1 (hashdb.db.commands/get id2)]
      (hashdb.db.commands/update-diff! m2-1 (into m2-1 {:uppsala 20 :sundsvall 30})))
    (hashdb.db.commands/delete-by-id! id2)
    (hashdb.db.commands/delete! (hashdb.db.commands/get id1))
    (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:bar "foo", :gunnar "lena"})
    (hashdb.db.commands/delete-by-id-with-minimum-history! id3)
    (println "m1")
    (clojure.pprint/pprint (hashdb.db.commands/history id1))
    (println "m2")
    (clojure.pprint/pprint (hashdb.db.commands/history id2))
    (println "m2-short")
    (clojure.pprint/pprint (hashdb.db.commands/history-short id2))
    (println "m3")
    (clojure.pprint/pprint (take 10 (hashdb.db.commands/history-nil-entity)))))


;; bugs:

;; 1. (hashdb.db.commands/history-by-entity nil) not working, returns () => fixed
