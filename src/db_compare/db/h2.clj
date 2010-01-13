(ns db-compare.db.h2
  (:import (java.sql DriverManager Connection Statement ResultSet))
  (:use (clojure.contrib [str-utils :only (str-join)])))

(defn- init []
  (let [host        "127.0.0.1"
        conn-string (str "jdbc:h2:tcp://" host "/benchdb;MULTI_THREADED=1")]
    {:host host :conn-string conn-string}))

(defn- open-client [{:keys [conn-string]}]
  (DriverManager/getConnection conn-string "sa" nil))

(defn- close-client [#^Connection conn]
  (.close conn))

(defn- setup [#^Connection conn]
  (with-open [stmt (.createStatement conn)]
    (.executeUpdate stmt
      "CREATE TABLE IF NOT EXISTS benchtable(
         id BIGINT,
         token VARCHAR(1000),
         birthdate BIGINT,
         rating DOUBLE,
         admin BOOLEAN)")
    (.execute stmt
      (str "CREATE INDEX IF NOT EXISTS benchtable_by_birthdate "
             "ON benchtable (birthdate)"))))

(defn- clear [#^Connection conn]
  (with-open [stmt (.createStatement conn)]
    (.executeUpdate stmt
      "DELETE FROM benchtable")))

(defn- values-str [record]
  (str "("
    (record "id") ","
    "'" (record "token") "',"
    (record "birthdate") ","
    (record "rating") ","
    (if (record "admin") "TRUE" "FALSE") ")"))

(defn- insert-one [#^Connection conn record]
  (with-open [stmt (.createStatement conn)]
    (.executeUpdate stmt
      (str "INSERT INTO benchtable VALUES "
        (values-str record)))))

(defn- insert-multiple [#^Connection conn records]
  (with-open [stmt (.createStatement conn)]
    (.executeUpdate stmt
      (str "INSERT INTO benchtable VALUES "
        (str-join "," (map #(values-str %) records))))))

(defn- rs-record [#^ResultSet rs]
  {"id"        (.getLong    rs 1)
   "token"     (.getString  rs 2)
   "birthdate" (.getLong    rs 3)
   "rating"    (.getDouble  rs 4)
   "admin"     (.getBoolean rs 5)})

(defn- rs-record-seq [#^ResultSet rs]
  (lazy-seq
    (when (.next rs)
      (cons (rs-record rs) (rs-record-seq rs)))))

(defn- get-one [#^Connection conn id]
  (with-open [stmt (.createStatement conn)]
    (let [rs (.executeQuery stmt
               (str "SELECT * FROM benchtable WHERE id = " id))]
    (.next rs)
    (rs-record rs))))

(defn- get-multiple [#^Connection conn ids]
  (with-open [stmt (.createStatement conn)]
    (let [rs (.executeQuery stmt
               (str "SELECT * FROM benchtable WHERE id IN ("
                      (str-join "," ids) ")"))]
    (dorun (rs-record-seq rs)))))

(defn- find-one [#^Connection conn birthdate]
  (with-open [stmt (.createStatement conn)]
    (let [rs (.executeQuery stmt
               (str "SELECT * FROM benchtable "
                       "WHERE birthdate = " birthdate " "
                       "LIMIT 1"))]
      (.next rs)
      (rs-record rs))))

(defn- find-multiple [#^Connection conn birthdate limit]
  (with-open [stmt (.createStatement conn)]
    (let [rs (.executeQuery stmt
               (str "SELECT * FROM benchtable "
                      "WHERE birthdate >= " birthdate " "
                      "LIMIT " limit))]
      (dorun (rs-record-seq rs)))))

(def h2-impl
  {:name "h2"
   :init init :open-client open-client :close-client close-client
   :setup setup :clear clear
   :insert-one insert-one :insert-multiple insert-multiple
   :get-one get-one :get-multiple get-multiple
   :find-one find-one :find-multiple find-multiple})
