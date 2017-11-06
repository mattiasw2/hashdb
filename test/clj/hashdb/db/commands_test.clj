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

(defn clear-database
  "Clear the database by reconstructing it from scratch."
  []
  (doseq [x (range 10)] (user/rollback))
  (user/migrate))


(defn random-string
  []
  (str (uuid)))


;; (create-update-operation {:a 1 :b 2 :c 3 :d 4})
;; => {:a "5280dda4-40ac-41f4-99a5-7a7131ee6582", :b "7076d6a7-2d78-4f2b-b38a-139662904a9c"}
;; (create-update-operation {:a 1 :b 2 :c 3 :d 4})
;; =>{:b nil}

(s/fdef create-update-operation
        :args (s/or :base (s/cat :m any?) ;; :hashdb.db.commands/data doesn't work since just map-like
                    :rec  (s/cat :m any? :changes :hashdb.db.commands/changes))
        :ret  :hashdb.db.commands/changes)

;; todo: add new k-v pairs, incl :s1 :s2 :s3 :s4
(defn create-update-operation
  "Change or delete or add a new key to `m`.
   In 30% of the operations, then value will be nil, i.e. delete."
  ([m] (create-update-operation (filter #(not (#{:id :entity :k :updated :version} (key %))) m) {}))
  ([m changes]
   (if (<= (count m) 3) changes
       (let [nxt (rand-int (count m))
             next (nth m nxt)
             rest (nthrest m nxt)
             k (key next)
             v (if (and (not (#{:s1 :s2 :s3 :s4 :s5} k))(< (rand-int 10) 3)) nil (random-string))]
             ;; v (random-string)]
         (recur rest (assoc changes k v))))))


(defn- test-many-continue
  [samples]
  (let [saved-m2s (timed "create!" (mapv create! samples))
        ids (map :id saved-m2s)
        _ (hashdb.db.commands/verify-these ids)
        saved-m3s (timed "update" (mapv #(update! % (create-update-operation %)) saved-m2s))
        _ (hashdb.db.commands/verify-these ids)
        saved-m4s (timed "delete" (mapv #(delete! %) saved-m3s))
        _ (hashdb.db.commands/verify-these ids)]))

(defn test-many
  [& [n]]
  (clear-database)
  (let [n (or n 1000)
        samples (timed "exercise" (mapv first (s/exercise :hashdb.db.commands/data n)))]
    (test-many-continue samples)))

;; MySQLTransactionRollbackException Deadlock found when trying to get lock; try restarting transaction  com.mysql.cj.jdbc.exceptions.SQLError.createSQLException (SQLError.java:539)
;; in create!
;; when running (test-many-parallel :n 100 :par 10 :delay 2000)
;; and (test-many-parallel :n 100 :par 10 :delay 500)  on the 9th thread doing create!
(defn test-many-parallel
  [& {:keys [n par delay]
      :or {n 1000
           par 2
           delay 2000}}]
  (let [n (or n 1000)
        samples (timed "exercise" (mapv first (s/exercise :hashdb.db.commands/data n)))]
    (clear-database)
    (->>
     (range par)
     (mapv (fn [_]
             (Thread/sleep delay)
             (future (test-many-continue samples))))
     (mapv deref))
    (println "Finished")))






;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; with string_index and without transactions
;; "Timed: exercise : 75463.495381 msecs"
;; "Timed: create! : 21870.207466 msecs"
;; "Timed: update : 10089.15617 msecs"
;; "Timed: delete : 13598.558028 msecs"

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; with transactions, no speed difference except create!, which is faster.
;; "Timed: exercise : 74817.655669 msecs"
;; "Timed: create! : 15552.223304 msecs"
;; "Timed: update : 10360.828127 msecs"
;; "Timed: delete : 11096.798229 msecs"




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


(deftest test-all-commands-without-indexes
  []
  (let [m1  (hashdb.db.commands/create! {:då "foo"})
        id1 (:id m1)
        m2  (hashdb.db.commands/create! {:bar "rolf" :uppsala 10})
        id2 (:id m2)
        m3  (hashdb.db.commands/create! {:gunnar "mattias"})
        id3 (:id m3)]
    (hashdb.db.commands/update! (hashdb.db.commands/get id1) {:bar "foo"})
    (let [m2-1 (hashdb.db.commands/get id2)
          res2 (hashdb.db.commands/update-diff! m2-1 (into m2-1 {:uppsala 20 :sundsvall 30}))]
      (is (= 30 (:sundsvall res2))))
    (is (nil? (hashdb.db.commands/delete-by-id! id2)))
    (is (nil? (hashdb.db.commands/delete! (hashdb.db.commands/get id1))))

    ;, for data element
    (is (= "lena" (:gunnar (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:bar "foo", :gunnar "lena"}))))
    (is (nil? (:bar (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:bar nil}))))
    (is (nil? (:bar (hashdb.db.commands/get id3))))
    ;; verify that nil values are removed from map
    ;; (is (not (contains? (hashdb.db.commands/get id3) :bar)))

    ;; for INDEXED data element
    (is (= "runar" (:s1 (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:s1 "runar"}))))
    (is (nil? (:s1 (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:s1 nil}))))
    (is (nil? (:s1 (hashdb.db.commands/get id3))))
    ;; verify that nil values are removed from map
    ;; (is (not (contains? (hashdb.db.commands/get id3) :s1)))

    (is (nil? (hashdb.db.commands/delete-by-id-with-minimum-history! id3)))
    (is (= 3 (count (hashdb.db.commands/history id1))))
    (is (= 3 (count (hashdb.db.commands/history id2))))
    (is (= 3 (count (hashdb.db.commands/history-short id2))))
    (println "m3")
    (is (< 4 (count (take 10 (hashdb.db.commands/history-nil-entity)))))))

(deftest test-all-commands-with-indexes
  []
  (let [m1  (hashdb.db.commands/create! {:s2 "foo"})
        id1 (:id m1)
        m2  (hashdb.db.commands/create! {:s3 "rolf" :uppsala 10})
        id2 (:id m2)
        m3  (hashdb.db.commands/create! {:s1 "mattias"})
        id3 (:id m3)]
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (hashdb.db.commands/update! (hashdb.db.commands/get id1) {:s3 "foo"})
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (let [m2-1 (hashdb.db.commands/get id2)
          res2 (hashdb.db.commands/update-diff! m2-1 (into m2-1 {:uppsala 20 :sundsvall 30}))]
      (is (= 30 (:sundsvall res2))))
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (is (nil? (hashdb.db.commands/delete-by-id! id2)))
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (is (nil? (hashdb.db.commands/delete! (hashdb.db.commands/get id1))))
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (is (= "lena" (:s1 (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:s3 "foo", :s1 "lena"}))))
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (is (nil? (hashdb.db.commands/delete-by-id-with-minimum-history! id3)))
    (is (verify-stored-data id1))
    (is (verify-stored-data id2))
    (is (verify-stored-data id3))
    (is (= 3 (count (hashdb.db.commands/history id1))))
    (is (= 3 (count (hashdb.db.commands/history id2))))
    (is (= 3 (count (hashdb.db.commands/history-short id2))))
    (println "m3")
    (is (< 4 (count (take 10 (hashdb.db.commands/history-nil-entity)))))))

(deftest test-all-commands-with-indexes-small
  []
  (let [m3  (hashdb.db.commands/create! {:s1 "mattias"})
        id3 (:id m3)]
    (is (verify-stored-data "unknown-id"))
    (is (verify-stored-data id3))
    (is (= "lena" (:s1 (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:s3 "foo", :s1 "lena"}))))
    (is (verify-stored-data id3))
    (is (= 2 (count (cmd/select-string-index {:id id3}))))
    (let [found   (select-by-string :unknown :s3 "foo")
          ntfound (select-by-string :unknown :s3 "foox")]
      (is (= "lena" (:s1 (first found))))
      (is (empty? ntfound)))))



;; bugs:

;; 1. (hashdb.db.commands/history-by-entity nil) not working, returns () => fixed


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; index testing


(deftest cycle-string-index-operations
  (let [id (uuid)
        entity ":nil"
        k ":hej"]
    (is (= 1 (hashdb.db.core/create-string-index! {:id id :entity entity :k k :index_data "foo"})))
    (is (= 1 (hashdb.db.core/update-string-index! {:id id :entity entity :k k :index_data "bar"})))
    (is (= 1 (hashdb.db.core/delete-string-index! {:id id :entity entity})))))


(deftest cycle-string-index-operations-upsert
  (let [id (uuid)
        entity ":nil"
        k ":hej"]
    (is (= 1 (hashdb.db.core/upsert-string-index! {:id id :entity entity :k k :index_data "foo"})))
    ;; Here you get a 2 rows affected, the first is the failed insert, the 2nd the update
    (is (= 2 (hashdb.db.core/upsert-string-index! {:id id :entity entity :k k :index_data "bar"})))
    (is (= 1 (hashdb.db.core/delete-string-index! {:id id :entity entity})))))

(deftest cycle-string-index-operations-upsert-using-replace
  (let [id (uuid)
        entity ":nil"
        k ":hej"]
    (is (= 1 (hashdb.db.core/upsert-string-index-using-replace! {:id id :entity entity :k k :index_data "foo"})))
    ;; Here you get a 2 rows affected, the first is the failed insert, the 2nd the update
    (is (= 2 (hashdb.db.core/upsert-string-index-using-replace! {:id id :entity entity :k k :index_data "bar"})))
    (is (= 1 (hashdb.db.core/delete-string-index! {:id id :entity entity})))))
