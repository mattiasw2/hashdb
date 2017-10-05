(ns ^:figwheel-no-load hashdb.app
  (:require [hashdb.core :as core]
            [devtools.core :as devtools]))

(enable-console-print!)

(devtools/install!)

(core/init!)
