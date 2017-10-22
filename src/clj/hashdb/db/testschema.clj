(ns hashdb.db.testschema
  (:require
   [clojure.spec.alpha :as s]
   [clj-time.jdbc]
   [clojure.java.jdbc :as jdbc]
   [clojure.java.jdbc :as sql]
   [hashdb.config :refer [env]]
   [hashdb.db.core :as cmd])
  (:import [java.sql
            BatchUpdateException
            PreparedStatement]))


;; why not use spec, i.e. say that schema should be a spec, whose topmost is a map
;; Things you want to have indexes has to be typed by a spec. (string? float? doible? int? timestamp?
;; get-spec reads spec (registry is all)

(s/def :ins/id string?)
(s/def :ins/version int?)

(s/def :ins/first-name (s/and string? #(<= (count %) 20)))

;; the main of :ins
(s/def :ins/entity map?)

(s/def :ins/t (s/keys :req [:ins/id :ins/version]))


;; (= (s/describe (s/get-spec :ins/id)) 'string?)
;; => true

;; (= (s/describe (s/get-spec :ins/entity)) 'map?)
;; => true

;; (= 'keys (first (s/describe (s/get-spec :ins/t))))
;; true


;; (s/describe (s/get-spec :ins/first-name))
;; => (and string? (<= (count %) 20))

;; (= 'and (first (s/describe (s/get-spec :ins/first-name))))
;; => true

;; (= 'string? (second (s/describe (s/get-spec :ins/first-name))))
;; => true
