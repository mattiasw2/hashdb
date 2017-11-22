(defproject hashdb "0.1.0"

  :description "HASHDB: A simple way of storing clojure maps into mySQL."
  :url "https://github.com/mattiasw2/hashdb"

  :dependencies [[clj-time "0.14.0"]
                 [conman "0.6.9"]
                 [cprop "0.1.11"]
                 [luminus-migrations "0.4.2"]
                 [mount "0.1.11"]
                 [org.clojure/core.cache "0.6.5"]
                 ;; cannot be upgraded to 8.0.8, since it doesn't exist
                 [mysql/mysql-connector-java "6.0.5"]
                 [org.clojure/clojure "1.9.0-RC1"]
                 [org.clojure/java.jdbc "0.7.3"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/tools.logging "0.4.0"]
                 ;; make instrumentation check :ret and :fn too
                 [orchestra "2017.11.12-1"]]

  :min-lein-version "2.0.0"

  :jvm-opts ["-server" "-Dconf=.lein-env"]
  :source-paths ["src/clj" "src/cljc"]
  :test-paths ["test/clj"]
  :resource-paths ["resources" "target/cljsbuild"]
  :target-path "target/%s/"
  :main ^:skip-aot hashdb.core
  :migratus {:store :database :db ~(get (System/getenv) "DATABASE_URL")}

  :plugins [[lein-cprop "1.0.3"]
            [migratus-lein "0.5.2"]]

  :clean-targets ^{:protect false}
  [:target-path [:cljsbuild :builds :app :compiler :output-dir] [:cljsbuild :builds :app :compiler :output-to]]

  :profiles
  {:uberjar {:omit-source true
             :prep-tasks ["compile"]
             :aot :all
             :uberjar-name "hashdb.jar"
             :source-paths ["env/prod/clj"]
             :resource-paths ["env/prod/resources"]}

   :dev           [:project/dev :profiles/dev]
   :test          [:project/dev :project/test :profiles/test]

   :project/dev  {:dependencies []
                  :plugins      [[com.jakemccrary/lein-test-refresh "0.19.0"]
                                 [lein-doo "0.1.8"]]
                  :doo {:build "test"}
                  :source-paths ["env/dev/clj"]
                  :resource-paths ["env/dev/resources"]
                  :repl-options {:init-ns user}}
   :project/test {:resource-paths ["env/test/resources"]}

   :profiles/dev {}
   :profiles/test {}})
