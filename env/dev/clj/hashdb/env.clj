(ns hashdb.env
  (:require [selmer.parser :as parser]
            [clojure.tools.logging :as log]
            [hashdb.dev-middleware :refer [wrap-dev]]))

(def defaults
  {:init
   (fn []
     (parser/cache-off!)
     (log/info "\n-=[hashdb started successfully using the development profile]=-"))
   :stop
   (fn []
     (log/info "\n-=[hashdb has shut down successfully]=-"))
   :middleware wrap-dev})
