(ns hashdb.doo-runner
  (:require [doo.runner :refer-macros [doo-tests]]
            [hashdb.core-test]))

(doo-tests 'hashdb.core-test)

