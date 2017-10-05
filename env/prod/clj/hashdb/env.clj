(ns hashdb.env
  (:require [clojure.tools.logging :as log]))

(def defaults
  {:init
   (fn []
     (log/info "\n-=[hashdb started successfully]=-"))
   :stop
   (fn []
     (log/info "\n-=[hashdb has shut down successfully]=-"))
   :middleware identity})
