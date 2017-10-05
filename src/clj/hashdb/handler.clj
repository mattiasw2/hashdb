(ns hashdb.handler
  (:require [compojure.core :refer [routes wrap-routes]]
            [hashdb.layout :refer [error-page]]
            [hashdb.routes.home :refer [home-routes]]
            [hashdb.routes.services :refer [service-routes]]
            [compojure.route :as route]
            [hashdb.env :refer [defaults]]
            [mount.core :as mount]
            [hashdb.middleware :as middleware]))

(mount/defstate init-app
                :start ((or (:init defaults) identity))
                :stop  ((or (:stop defaults) identity)))

(def app-routes
  (routes
    (-> #'home-routes
        (wrap-routes middleware/wrap-csrf)
        (wrap-routes middleware/wrap-formats))
    #'service-routes
    (route/not-found
      (:body
        (error-page {:status 404
                     :title "page not found"})))))


(defn app [] (middleware/wrap-base #'app-routes))
