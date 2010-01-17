(ns db-compare.db.postgresql
  (:import (java.sql DriverManager Connection Statement))
  (:use (clojure.contrib [def :only (defvar-)]
                         [str-utils :only (str-join)])
        (db-compare.db sql)))

(defn- update-multiple-with-retry [#^Statement stmt query]
  (try
    (.executeUpdate stmt query)
    (catch Exception e
      (if (.contains (.getMessage e) "deadlock")
        (do
          (println "retry" (.getId (Thread/currentThread)))
          (update-multiple-with-retry stmt query))
        (throw e)))))

(defvar- postgresql-custom-impl {
  :init
  (fn []
    (Class/forName "org.postgresql.Driver")
    (let [host        "127.0.0.1"
          port        5432
          username    "mmcgrana"
          password    nil
          conn-string (str "jdbc:postgresql://" host ":" port "/benchdb")]
      {:host host :port port :username username :password password
       :conn-string conn-string}))

  :ensure-collection
  (fn [#^Connection conn coll]
    (with-open [stmt (.createStatement conn)]
      (.executeUpdate stmt
        (str "DROP TABLE IF EXISTS " coll))
      (.executeUpdate stmt
        (str "CREATE TABLE " coll "(
                id BIGINT,
                token VARCHAR(1000),
                birthdate BIGINT,
                rating DOUBLE PRECISION,
                admin BOOLEAN)"))))

  :update-multiple
  ; can't use this because of dadlock
  ;(fn [#^Connection conn coll ids attr val]
  ;  (let [query (str "UPDATE " coll " "
  ;                     "SET " attr " = " val " "
  ;                     "WHERE id IN (" (str-join "," (sort ids)) ")")]
  ;    (with-open [stmt (.createStatement conn)]
  ;      (update-multiple-with-retry stmt query))))
  nil
})

(def postgresql-impl (merge sql-impl postgresql-custom-impl))
