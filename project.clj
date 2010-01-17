(defproject fleetdb "0.1.0-SNAPSHOT"
  :description "A document-oriented database optimized for agile development."
  :url "http://github.com/mmcgrana/fleetdb"
  :repositories {"tokyotyrant-java"  "http://tokyotyrant-java.googlecode.com/svn/maven/repository"
                 "de.fforw.releases" "http://fforw.de/m2repo/releases/"
                 "racleReleases"     "http://download.oracle.com/maven"}
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0-master-SNAPSHOT"]
                 [clj-json "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[clj-stacktrace "0.1.0-SNAPSHOT"]
                     [fleetdb "0.1.0"]
                     [fleetdb-client "0.1.0"]
                     [org.clojars.mmcgrana/jredis-core-api "a.0-SNAPSHOT"]
                     [org.clojars.mmcgrana/jredis-core-ri "a.0-SNAPSHOT"]
                     [org.clojars.mmcgrana/mongo "1.2"]
                     [org.clojars.mmcgrana/java-memcached "2.0.1"]
                     [com.h2database/h2 "1.2.126"]
                     [mysql/mysql-connector-java "5.1.9"]
                     [postgresql/postgresql "8.4-701.jdbc4"]])
