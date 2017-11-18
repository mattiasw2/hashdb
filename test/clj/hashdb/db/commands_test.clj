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
   [luminus-migrations.core :as migrations]
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
  (println ".")
  (migrations/migrate ["reset"] (select-keys env [:database-url])))


(defn random-string
  []
  (str (uuid)))


(defn- update-or-delete-value
  [k]
  ;; for now, we cannot delete indexed keys, since ::s1 etc is not nilable
  ;; (cond (and (< (rand-int 10) 3) (not (possible-keys k))) nil)
  (cond (< (rand-int 10) 3) nil
        (#{:i1 :i2 :i3 :i4} k)                                             (rand-int 2147483647)
        ;; true for all other data incl :s1 :s2 :s3 :s4
        true                                                                   (random-string)))


;; (create-update-operation {:a 1 :b 2 :c 3 :d 4})
;; => {:a "5280dda4-40ac-41f4-99a5-7a7131ee6582", :b "7076d6a7-2d78-4f2b-b38a-139662904a9c"}
;; (create-update-operation {:a 1 :b 2 :c 3 :d 4})
;; =>{:b nil}

(s/fdef create-update-operation
        :args (s/or :base (s/cat :m :hashdb.db.commands/data)
                    ;; not ::data any more on recursion, since we called filter
                    :rec  (s/cat :m any? :changes :hashdb.db.commands/changes))
        :ret  :hashdb.db.commands/changes)



(defn create-update-operation
  "Change or delete or add a new key to `m`.
   In 30% of the operations, then value will be nil, i.e. delete."
  ;; Do not change or delete any of the pre-defined keys
  ([m] (let [res (create-update-operation (filter #(not (#{:id :tenant :entity :k :updated :version} (key %))) m) {})]
         ;; (println (count res))
         res))
  ([m changes]
   (if (<= (count m) 3) changes
       ;; find the next key to change (update or delete)
       (let [nxt      (rand-int (count m))
             next     (nth m nxt)
             rest     (nthrest m (min 1 nxt)) ;0 just mean that I am regenerating the first key.
             k        (key next)
             _        (assert (some? k))
             v        (update-or-delete-value k)
             changes2 (assoc changes k v)]
         ;; see if we should add a new key to the map
         (if (< (rand-int 10) 2)
           (recur rest (let [k2 (keyword (random-string))
                             v2 (update-or-delete-value k2)]
                         (assoc changes2 k2 v2)))
           (recur rest changes2))))))


;; (count-by-keys [{:s1 1 :k 2}{:s1 2 :s2 3 :l 4}])
;; ==> {:s4 0, :s1 2, :s3 0, :s2 1}
(defn count-by-keys
  [ms]
  (->>
   (for [k possible-keys]
     [k (count (filter (fn [m] (k m)) ms))])
   (into {})))


;; (by-keys [{:s1 1 :k 2  :id "A"}{:s1 2 :s2 3 :l 4 :id "B"}{:s2 3 :id "C"}])
;; => {:s4 (), :s1 ({:id "A", :s1 1} {:id "B", :s1 2}), :s3 (), :s2 ({:id "B", :s2 3} {:id "C", :s2 3})}
(defn by-keys
  [ms]
  (->>
   (for [k ms]
     [k (map #(select-keys % [:id k])(filter (fn [m] (k m)) ms))])
   (into {})))

;; tested by just deleting a row in string_index, and then I get warnings like
;; FAIL in () (commands_test.clj:138)
;; expected: 2
;;   actual: 1
;;     diff: - 2
;;           + 1

(defn verify-indexes
  "Verify that string_index entries are in sync with latest.
   It is done by looking up all indexed values and make sure all are found."
  [ms]
  (doseq [[k expected](by-keys ms)]
    (let [by-value (group-by k expected)]
      (doseq [[expected ms] by-value]
        (assert (some? expected))
        ;; this code will only work if we have 1 thread
        (let [hits (select-by :unknown k expected)
              hits-set (into #{} (map :id hits))
              ms-set (into #{} (map :id ms))]
          (assert (and
                   (= (count hits)(count ms))
                   (= hits-set ms-set))))))))



(defn- test-many-continue
  [samples single]
  (let [saved-m2s (timed "create!" (mapv create! samples))
        ids (map :id saved-m2s)
        _ (hashdb.db.commands/verify-these ids)
        _ (when single (verify-indexes saved-m2s))

        ;; todo, maybe we shouldn't update them all
        saved-m3s0 (timed "update" (mapv #(update! % (create-update-operation %)) saved-m2s))
        _ (hashdb.db.commands/verify-these ids)
        _ (when single (verify-indexes saved-m3s0))
        saved-m3s (timed "update-2" (mapv #(update! % (create-update-operation %)) saved-m3s0))
        _ (hashdb.db.commands/verify-these ids)
        _ (when single (verify-indexes saved-m3s))

        ;; for delete, make a number of deletes, and verify we didn't destroy the rest
        [left right] (split-at (/ (count saved-m3s) 3) saved-m3s)
        _ (timed "delete" (mapv #(delete! %) left))
        _ (hashdb.db.commands/verify-these ids)
        _ (when single (verify-indexes right))
        _ (timed "delete" (mapv #(delete! %) right))
        ;; todo: verify-indexes after delete by usong saved-m3s and make sure nothing is found
        _ (hashdb.db.commands/verify-these ids)]))



(s/fdef test-many
        :args (s/cat :samples (s/coll-of :hashdb.db.commands/data))
        :ret  nil?)

(defn test-many
  "Test 1 thread."
  [samples]
  (clear-database)
  (test-many-continue samples true))

(def ^:dynamic src nil)
(defn test-many-n
  [n]
  (with-tenant :single
    (let [d (mapv first (s/exercise :hashdb.db.commands/data n))]
      (def src d)
      (test-many d))))

;; I cannot run this in multiple-thread, and inside check there is a pmap.
;; So I am testing check-fn instead.
;; https://spootnik.org/entries/2017/01/09/an-adventure-with-clocks-component-and-spec/
(defn check-test-many
  []
  (stest/check-fn `test-many (s/fspec :args (s/cat :samples (s/coll-of :hashdb.db.commands/data)) :ret  nil?)))

;; MySQLTransactionRollbackException Deadlock found when trying to get lock; try restarting transaction  com.mysql.cj.jdbc.exceptions.SQLError.createSQLException (SQLError.java:539)
;; in create!
;; when running (test-many-parallel :n 100 :par 10 :delay 2000)
;; and (test-many-parallel :n 100 :par 10 :delay 500)  on the 9th thread doing create!
(defn test-many-parallel
  "Test in parallel.
   If just one thread, the indexes will also be verified."
  [& {:keys [n par delay]
      :or {n 1000
           par 2
           delay 2000}}]
  (with-tenant :single
    (let [samples (timed "exercise" (mapv first (s/exercise :hashdb.db.commands/data n)))]
      (clear-database)
      (->>
       (range par)
       (mapv (fn [idx]
               (Thread/sleep delay)
               ;; it seems as if a future call also get the active thread-bindings.
               ;; in any normal language, I would have to have the with-tenant part inside the future
               ;; the with-tenant is only needed if we have several tenants, otherwise it will work
               (if (= par 1)
                 (future (test-many-continue samples (= par 1)))
                 (with-tenant (str idx)
                   (future (test-many-continue samples (= par 1)))))))
       (mapv deref))
      (println "Finished"))))






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

(deftest test-cannot-get-from-other-tennant
  (clear-database)
  (let [m (with-tenant "one"
            (hashdb.db.commands/create! {:bergstr�m "mats"}))
        mA (with-tenant "eleven"
             (hashdb.db.commands/create! {:bergstr�m "mats"}))
        mB (with-tenant "twelfe"
             (hashdb.db.commands/create! {:bergstr�m "mats"}))
        m2 (with-tenant "one"
             (hashdb.db.commands/get (:id m)))
        _  (is (= (:id m) (:id m2)))
        m3 (with-tenant :global
             (hashdb.db.commands/get (:id m)))
        _  (is (= (:id m) (:id m3)))
        m4 (with-tenant "two"
             (is (thrown? Throwable (hashdb.db.commands/get (:id m)))))
        m5 (with-tenant "two"
             (is (thrown? Throwable (hashdb.db.commands/update! m {:more 10}))))
        m6 (with-tenant "one"
             (hashdb.db.commands/update! m {:more 10}))
        _ (is (= (:more m6) 10))
        m7 (with-tenant "two"
             (is (thrown? Throwable (hashdb.db.commands/delete! m))))
        m8 (with-tenant "one"
             (hashdb.db.commands/delete! m))
        _ (is (nil? m8))
        m9 (with-tenant "two"
             (is (thrown? Throwable (hashdb.db.commands/delete-by-id! (:id mA)))))
        m10 (with-tenant "eleven"
              (hashdb.db.commands/delete-by-id! (:id mA)))
        _ (is (nil? m10))
        m11 (with-tenant "two"
              (is (thrown? Throwable (hashdb.db.commands/delete-by-id-with-minimum-history! (:id mB)))))
        m12 (with-tenant "twelfe"
              (hashdb.db.commands/delete-by-id-with-minimum-history! (:id mB)))
        _ (is (nil? m12))]))

(deftest test-cannot-select-from-other-tennant
  (clear-database)
  (let [m (with-tenant "two"
            (hashdb.db.commands/create! {:s1 "mats" :name "bergstr�m"}))
        m2 (with-tenant "two"
             (hashdb.db.commands/create! {:s1 "mats" :name "johansson"}))
        m3 (with-tenant "three"
             (hashdb.db.commands/create! {:s1 "mats" :name "larsson"}))
        ms (with-tenant "two"
             (hashdb.db.commands/select-by :unknown :s1 "mats"))
        _  (is (= 2 (count ms)))
        ms2 (with-tenant "three"
              (hashdb.db.commands/select-by :unknown :s1 "mats"))
        _  (is (= 1 (count ms2)))
        ms3 (with-tenant :global
              (hashdb.db.commands/select-by-global :unknown :s1 "mats"))
        _  (is (= 3 (count ms3)))
        ms4 (with-tenant "four"
              (is (thrown? Throwable    ;Exception not enough, since assert
                           (hashdb.db.commands/select-by-global :unknown :s1 "mats"))))]))









(deftest test-all-commands-without-indexes
  (clear-database)
  (with-tenant :single
    (let [m1  (hashdb.db.commands/create! {:d� "foo"})
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
      (is (< 4 (count (take 10 (hashdb.db.commands/history-nil-entity))))))))




(deftest test-all-commands-with-string-indexes
  []
  (with-tenant :single
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
      (is (< 4 (count (take 10 (hashdb.db.commands/history-nil-entity))))))))

(deftest test-all-commands-with-string-indexes-small
  (with-tenant :single
    (let [m3  (hashdb.db.commands/create! {:s1 "mattias"})
          id3 (:id m3)]
      (is (verify-stored-data "unknown-id"))
      (is (verify-stored-data id3))
      (is (= "lena" (:s1 (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:s3 "foo", :s1 "lena"}))))
      (is (verify-stored-data id3))
      (is (= 2 (count (cmd/select-string-index {:id id3}))))
      (let [found   (select-by :unknown :s3 "foo")
            ntfound (select-by :unknown :s3 "foox")]
        (is (= "lena" (:s1 (first found))))
        (is (empty? ntfound))))))

(deftest test-all-commands-with-long-indexes-small
  (clear-database)
  (with-tenant :single
    (let [m3  (hashdb.db.commands/create! {:i1 567})
          id3 (:id m3)]
      (is (verify-stored-data id3))
      (is (= 10000 (:i1 (hashdb.db.commands/update! (hashdb.db.commands/get id3) {:i3 678, :i1 10000}))))
      (is (verify-stored-data id3))
      (is (verify-stored-data "unknown-id"))
      (is (= 2 (count (concat (cmd/select-string-index {:id id3})(cmd/select-long-index {:id id3})))))
      (let [found   (select-by :unknown :i3 678)
            ntfound (select-by :unknown :i3 678678)]
        (is (= 10000 (:i1 (first found))))
        (is (empty? ntfound))))))

(deftest test-verify-row-in-sync
  (with-tenant :single
    (is (verify-row-in-sync {:updated (org.joda.time.DateTime. (now)) :entity ":f" :tenant SINGLE-TENANT} {:updated (now) :entity :f :tenant :single}))
    (let [start (now)]
      (Thread/sleep 1100)
      (is (thrown?
           Throwable
           (verify-row-in-sync {:updated (org.joda.time.DateTime. (now)) :entity ":f"} {:updated start :entity :f}))))))



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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; spec test when using filter on maps

(s/def ::test-id int?)
(s/def ::test-data string?)

(s/def ::test1
  (s/keys :req-un [::test-id ::test-data]))

(s/fdef spec-test
        :args (s/cat :m ::test1)
        :ret  int?)

(defn spec-test
  [m]
  (count m))

(def test1-sample {:test-id 1 :test-data "hello"})

(defn works
  []
  (spec-test test1-sample))

(defn works-not-which-is-ok
  []
  (spec-test (dissoc test1-sample :test-data)))

(defn works-not-which-is-not-ok
  []
  (spec-test (filter (fn [_] true) test1-sample)))


(orchestra.spec.test/instrument)
