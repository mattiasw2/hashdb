(ns hashdb.app
  (:require [hashdb.core :as core]))

;;ignore println statements in prod
(set! *print-fn* (fn [& _]))

(core/init!)
