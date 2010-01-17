(ns db-compare.db.mongodb
  (:use (clojure.contrib [def :only (defmacro-)]))
  (:import (com.mongodb Mongo DB DBCollection BasicDBObject DBObject DBCursor)
           (java.util List)))

(defmacro- to-coll [db-sym coll-sym & chain]
  `(-> ~db-sym (.getCollection ~coll-sym)
     ~@chain))

(defn- db-obj [#^IPersistentMap record]
  (let [id (record "id")]
    (BasicDBObject. (assoc (dissoc record "id") "_id" id))))

(def mongodb-impl {
  :init
  (fn []
    (let [host "127.0.0.1"
          port 27017]
      (Mongo. host port)))

  :open-client
  (fn [#^Mongo mongo]
    (doto (.getDB mongo "benchdb")
      (.requestStart)))

  :close-client
  (fn [#^DB db]
    (.requestDone db))

  :clear-collection
  (fn [#^DB db coll]
    (to-coll db coll
      (.drop)))

  :ensure-index
  (fn [#^DB db coll attr]
    (to-coll db coll
      (.ensureIndex (BasicDBObject. attr 1))))

  :insert-one
  (fn [#^DB db coll record]
    (to-coll db coll
      (.insert #^DBObject (db-obj record))))

  :insert-multiple
  (fn [#^DB db coll records]
    (to-coll db coll
      (.insert #^List (map db-obj records))))

  :get-one
  (fn [#^DB db coll id]
    (to-coll db coll
      (.findOne (BasicDBObject. "_id" id))))

  :get-multiple
  (fn [#^DB db coll ids]
    (to-coll db coll
      (.find (BasicDBObject. "_id"
               (BasicDBObject. "$in" (into-array ids))))))

  :find-one
  (fn [#^DB db coll attr value]
    (to-coll db coll
      (.findOne (BasicDBObject. attr value))))

  :find-above
  (fn [#^DB db coll attr val limit]
    (to-coll db coll
      (.find (BasicDBObject. attr (BasicDBObject. "$gt" val)))
      (.limit limit)))

  :find-above2
  (fn [#^DB db coll attr1 val1 attr2 val2 limit]
    (let [con (-> (BasicDBObject.)
                (.append attr1 (BasicDBObject. "$gt" val1))
                (.append attr2 (BasicDBObject. "$gt" val2)))]
      (to-coll db coll
        (.find con)
        (.limit limit))))

  :update-one
  (fn [#^DB db coll id attr val]
    (to-coll db coll
      (.update (BasicDBObject. "_id" id)
               (BasicDBObject. "$set" (BasicDBObject. attr val))
               false false)))

  :update-multiple
  (fn [#^DB db coll ids attr val]
    (to-coll db coll
      (.update (BasicDBObject. "$in"  (BasicDBObject."_id" ids))
               (BasicDBObject. "$set" (BasicDBObject. attr val))
               false true)))

  :delete-one
  (fn [#^DB db coll id]
    (to-coll db coll
      (.remove (BasicDBObject. "_id" id))))
})
