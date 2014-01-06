(defproject thrift-pail-cascalog-example "0.1.0-SNAPSHOT"
  :description "Example of using Thrift, Pail and Cascalog."
  :url "http://github.com/EricGebhart/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.hadoop/hadoop-core "1.2.0" ]
                 [cascalog "2.0.0" ]
                 [clj-pail "0.1.3"]
                 [clj-thrift "0.1.2"]
                 [pail-thrift "0.1.0"]
                 [pail-cascalog "0.1.0"]]

  :aot [thrift-pail-cascalog-example.data-unit-pail-structure]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0-master-SNAPSHOT"]]}

             :dev {:plugins [[lein-thriftc "0.1.0"]
                             [lein-midje "3.0.1"]]
                   :prep-tasks ["thriftc" "javac"]}}
  :java-source-paths ["src/thrift"]
  :main thrift-pail-cascalog-example.core)
