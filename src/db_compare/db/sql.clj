(ns db-compare.db.sql
  (:import (java.sql DriverManager Connection Statement ResultSet))
  (:use (clojure.contrib [str-utils :only (str-join)]
                         [def :only (defmacro-)])))

; todo: remove hard-coded table structure?

(defmacro- with-stmt [[stmt-sym conn-sym] & body]
  `(with-open [~stmt-sym (.createStatement ~conn-sym)]
     ~@body))

(defn- values-str [record]
  (str "("
    (record "id") ","
    "'" (record "token") "',"
    (record "birthdate") ","
    (record "rating") ","
    (if (record "admin") "TRUE" "FALSE") ")"))

(defn- rs-record [#^ResultSet rs]
  {"id"        (.getLong    rs 1)
   "token"     (.getString  rs 2)
   "birthdate" (.getLong    rs 3)
   "rating"    (.getDouble  rs 4)
   "admin"     (.getBoolean rs 5)})

(defn- rs-record-seq [#^ResultSet rs]
  (lazy-seq
    (if (.next rs)
      (cons (rs-record rs) (rs-record-seq rs)))))

(def sql-impl {
  :open-client
  (fn [{:keys [conn-string username password]}]
    (DriverManager/getConnection conn-string username password))

  :close-client
  (fn [#^Connection conn]
    (.close conn))

  :ensure-index
  (fn [#^Connection conn coll attr]
    (with-open [stmt (.createStatement conn)]
      (try
        (.execute stmt
          (str "CREATE INDEX " coll "_by_" attr
                 " ON " coll " (" attr")"))
        (catch com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException e))))

  :clear-collection
  (fn [#^Connection conn coll]
    (with-stmt [stmt conn]
      (.executeUpdate stmt
        (str "DELETE FROM " coll))))

  :insert-one
  (fn [#^Connection conn coll record]
    (with-stmt [stmt conn]
      (.executeUpdate stmt
        (str "INSERT INTO " coll " VALUES "
          (values-str record)))))

  :insert-multiple
  (fn [#^Connection conn coll records]
    (with-stmt [stmt conn]
      (.executeUpdate stmt
        (str "INSERT INTO " coll " VALUES "
          (str-join "," (map #(values-str %) records))))))

  :get-one
  (fn [#^Connection conn coll id]
    (with-stmt [stmt conn]
      (let [rs (.executeQuery stmt
                 (str "SELECT * FROM " coll " WHERE id = " id))]
        (if (.next rs)
          (rs-record rs)))))

  :get-multiple
  (fn [#^Connection conn coll ids]
    (with-stmt [stmt conn]
      (let [rs (.executeQuery stmt
                 (str "SELECT * FROM " coll " WHERE id IN ("
                        (str-join "," ids) ")"))]
        (rs-record-seq rs))))

  :find-one
  (fn [#^Connection conn coll attr val]
    (with-stmt [stmt conn]
      (let [rs (.executeQuery stmt
                 (str "SELECT * FROM " coll " "
                         "WHERE " attr " = " val " "
                         "LIMIT 1"))]
        (if (.next rs)
          (rs-record rs)))))

  :find-above
  (fn  [#^Connection conn coll attr val limit]
    (with-stmt [stmt conn]
      (let [rs (.executeQuery stmt
                 (str "SELECT * FROM " coll " "
                        "WHERE " attr " > " val " "
                        "LIMIT " limit))]
        (rs-record-seq rs))))

  :find-above2
  (fn  [#^Connection conn coll attr1 val1 attr2 val2 limit]
    (with-stmt [stmt conn]
      (let [rs (.executeQuery stmt
                 (str "SELECT * FROM " coll " "
                        "WHERE ((" attr1 " > " val1 ") AND "
                               "(" attr2 " > " val2 ")) "
                        "LIMIT " limit))]
        (rs-record-seq rs))))

  :update-one
  (fn [#^Connection conn coll id attr val]
    (with-stmt [stmt conn]
      (.executeUpdate stmt
        (str "UPDATE " coll " "
             "SET " attr " = " val " "
             "WHERE id = " id))))

  :delete-one
  (fn [#^Connection conn coll id]
    (with-stmt [stmt conn]
      (.executeUpdate stmt
        (str "DELETE FROM " coll " WHERE id = " id))))
})
