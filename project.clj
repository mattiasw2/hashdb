(defproject org.clojars.mattiasw/hashdb "0.1.6"

  :description "HASHDB: A simple way of storing clojure maps into mySQL."
  :url "https://github.com/mattiasw2/hashdb"

  :dependencies [[clj-time "0.14.2"]
                 [conman "0.7.5"]
                 [cprop "0.1.11"]
                 [luminus-migrations "0.5.0"]
                 [mount "0.1.11"]
                 [org.clojure/core.cache "0.6.5"]
                 ;; lein ancient doesn't know I have to add "-dmr" to the version
                 [mysql/mysql-connector-java "8.0.8-dmr"]
                 ;; [mysql/mysql-connector-java "6.0.6"]
                 [org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.5"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/tools.logging "0.4.0"]
                 ;; make instrumentation check :ret and :fn too
                 [orchestra "2017.11.12-1"]

                 ;; (mount/start) needs this in dev mode
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [ch.qos.logback/logback-core "1.2.3"]

                 ;; for Intellij and Cursive
                 [org.clojure/tools.nrepl "0.2.13"]
                 [clojure-complete "0.2.4"]

                 ;; testind infering of specs
                 [org.clojure/core.typed "0.4.3"]

                 [com.gfredericks/test.chuck "0.2.8"]


                 ;; needed for (hashdb.db.commands-test/test-many-n 10)
                 [org.clojure/test.check "0.10.0-alpha2"]]

  ;; only use `lein deploy`, not `lein deploy clojars`
  ;;
  ;; Now it doesn't call gpg any more.
  ;; Could not transfer artifact hashdb:hashdb:pom:0.1.4 from/to releases (https://clojars.org/mattiasw):
  ;; Access denied to: https://clojars.org/mattiasw/hashdb/hashdb/0.1.4/hashdb-0.1.4.pom, ReasonPhrase: Forbidden.
  :deploy-repositories [["releases"  {:sign-releases false :url "https://clojars.org"}]
                        ["snapshots" {:sign-releases false :url "https://clojars.org"}]]

  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "same as Clojure"}

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
