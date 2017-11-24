(ns mw.std
  (:require
   [schema.core :as sc]
   [clojure.spec.alpha :as s]
   [clojure.pprint :as pp]
   [clojure.tools.logging :as log]))

(defn substring+
  [str start end]
  (let [len (count str)
        start (min start len)
        end (min end len)]
    (.substring str start end)))


;; this error monad is cool too: https://brehaut.net/blog/2011/error_monads
(defmacro try*
  [& body]
  `(try [:ok (do ~@body)] (catch Exception e# [:error e#])))

;; just as mapv, i.e. non-lazy and return vector
(defmacro forv [& body]
  `(vec (for ~@body)))

(defn fetch
  "(fetch :foo x) == (:foo x) but aborts if result is nil"
  [key map]
  (let [res (get map key)]
    ;; (some? is the same as not-nil?
    (assert (some? res) (str key " not found in " map))
    res))

(defn nn
  "not-nil: abort if value nil"
  [v]
  (assert (some? v))
  v)

(defmacro def- [item value]
  `(def ^{:private true} ~item ~value))


;; works well
(defmacro loading []
  `(do (taoensso.timbre/info ~(str "loading " *file* " "  (:line (meta &form))))
       (set! *warn-on-reflection* true)
       (schema.core/set-fn-validation! true)))


(defmacro ignore-exception [& form]
  `(try ~@form (catch Exception exc# :ignored)))


;; (defmacro my-println [x]
;;   `(do (printf "%s:%s> %s is %s\n"
;;                ~*file*
;;                ~(:line (meta &form))
;;                ~(pr-str x)
;;                ~x)
;;        (flush)))


;; (alter-var-root #'pp/*print-suppress-namespaces* (constantly true))

(defn- t-3
  [frms]
  ;; for (binding we should have the value, not a function
  (binding [pp/*print-suppress-namespaces* true]
    (pp/pprint
     `(deftest ~(symbol (str (first frms) "-test"))
        (is (= '~(eval frms)
               ~frms))))))

(defmacro t
  "Create a test case by executing `frms`."
  [frms]
  (t-3 frms))



(defn call-and-log-exception-2
  [exc frm]
  (println (str "Exception: " exc))
  (prn frm)
  (log/error exc)
  (log/error "Failed call: " frm)
  (throw exc))

;; implemented using apply, to make sure code for arguments are only generated once.
;; this optimization might be unnecessary, since it might be that the java compiler does this itself.

;; (pp/pprint (macroexpand '(call-and-log-exception / 1 0)))
;; (let*
;;  [args__77723__auto__ [1 0]]
;;  (try
;;   (clojure.core/apply / args__77723__auto__)
;;   (catch
;;    java.lang.Throwable
;;    exc__77724__auto__
;;    (sss4.routes.migr-boapi-routes/call-and-log-exception-2
;;     exc__77724__auto__
;;     (clojure.core/list 'clojure.core/apply '/ args__77723__auto__)))))

;; sss4.routes.migr-boapi-routes> (let [x 2] (call-and-log-exception / 1 x))
;; 1/2
;; sss4.routes.migr-boapi-routes> (let [x 0] (call-and-log-exception / 1 x))
;; Exception: java.lang.ArithmeticException: Divide by zero
;; (clojure.core/apply / [1 0])
;; ArithmeticException Divide by zero  clojure.lang.Numbers.divide (Numbers.java:158)
(defmacro call-and-log-exception
  "Apply `f` to seq of `args0` like (call-and-log-exception / 1 0) and if throws exception,
   print the call so that it can be tested."
  [f & args0]
  `(let [args# [~@args0]]
     (try
       (apply ~f args#)
       (catch Throwable exc#
         (call-and-log-exception-2 exc# (list 'apply '~f args#))))))

(defn now-as-str
  "Just return a now as a nice string."
  []
  (.format (java.text.SimpleDateFormat. "yyyyMMddHHmm") (new java.util.Date)))

(defmacro timed
  "Just like time, but also prints the top-level function called"
  [txt expr]
  (let [sym (= (type expr) clojure.lang.Symbol)]
    `(let [start# (. System (nanoTime))
           return# ~expr
           res# (if ~sym
                  (resolve '~expr)
                  (resolve (first '~expr)))]
       (prn (str "Timed: " ~txt " "
                 (:name (meta res#))
                 ": " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
       return#)))
