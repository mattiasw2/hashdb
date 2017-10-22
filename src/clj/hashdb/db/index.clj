(ns hashdb.db.index
  (:require
   [clj-time.jdbc]
   [clojure.java.jdbc :as jdbc]
   [clojure.java.jdbc :as sql]
   [hashdb.config :refer [env]]
   [hashdb.db.core :as cmd])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))

;; How to map index:es to keys?
;;
;; 1. function f that maps :type to [keys type]
;; 2. Have a global list of keys (works well for SSS except for customer data)
;; 3. SSS user data needs custom, e.g. :typ=ins -> :frm -> [keys type]
;; 4. Have a default that indexes all of them
;; 5. ?
;;
;; Should I allow two column index:es?
;;
;; how to handle acc(ount)?
;;
;; how to know the type


;; In order to index, I need the type, where can it come from:
;; 1. declared
;; 2. clojure.spec
;; 3. guess from data (but then I will not know in which index to look.)
;; 4. ?
;; 5. ?

;; Terminology from datomic:

;; user=> (def movie-schema [{:db/ident :movie/title
;;                            :db/valueType :db.type/string
;;                            :db/cardinality :db.cardinality/one
;;                            :db/doc "The title of the movie"}

;;                           {:db/ident :movie/genre
;;                            :db/valueType :db.type/string
;;                            :db/cardinality :db.cardinality/one
;;                            :db/doc "The genre of the movie"}

;;                           {:db/ident :movie/release-year
;;                            :db/valueType :db.type/long
;;                            :db/cardinality :db.cardinality/one
;;                            :db/doc "The year the movie was released in theaters"}])

;; why not use spec, i.e. say that schema should be a spec, whose topmost is a map
;; Things you want to have indexes has to be typed by a spec. (string? float? doible? int? timestamp?
;; get-spec reads spec (registry is all)
