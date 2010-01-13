(ns db-compare.db.mongodb
  (:import (com.mongodb Mongo DB DBCollection BasicDBObject DBObject DBCursor)
           (java.util List)))

(defn- init []
  (let [host "127.0.0.1"
        port 27017]
    (Mongo. host port)))

(defn- open-client [#^Mongo mongo]
  (doto (.getDB mongo "benchdb")
    (.requestStart)))

(defn- close-client [#^DB db]
  (.requestDone db))

(defn- setup [#^DB db]
  (-> db (.getCollection "benchcoll")
    (.ensureIndex (BasicDBObject. "birthdate" 1))))

(defn- clear [#^DB db]
  (-> db (.getCollection "benchcoll")
    (.remove (BasicDBObject.))))

(defn- db-obj [#^IPersistentMap record]
  (let [id (record "id")]
    (BasicDBObject. (assoc (dissoc record "id") "_id" id))))

(defn- insert-one [#^DB db record]
  (-> db (.getCollection "benchcoll")
    (.insert #^DBObject (db-obj record))))

(defn- insert-multiple [#^DB db records]
  (-> db (.getCollection "benchcoll")
    (.insert #^List (map db-obj records))))

(defn- get-one [#^DB db id]
  (-> db (.getCollection "benchcoll")
    (.findOne (BasicDBObject. "_id" id))))

(defn- get-multiple [#^DB db ids]
  (-> db (.getCollection "benchcoll")
    (.find (BasicDBObject. "_id"
             (BasicDBObject. "$in" (into-array ids))))
    (.toArray)))

(defn- find-one [#^DB db birthdate]
  (-> db (.getCollection "benchcoll")
    (.findOne (BasicDBObject. "birthdate" birthdate))))

(defn- find-multiple [#^DB db birthdate find-multiple-size]
  (-> db (.getCollection "benchcoll")
    (.find (BasicDBObject. "birthdate" birthdate))
    (.limit find-multiple-size)
    (.toArray)))

(def mongodb-impl
  {:name "mongodb"
   :init init :open-client open-client :close-client close-client
   :setup setup :clear clear
   ;:insert-one insert-one :insert-multiple insert-multiple
   :insert-multiple insert-multiple
   :get-one get-one :get-multiple get-multiple
   :find-one find-one :find-multiple find-multiple})
