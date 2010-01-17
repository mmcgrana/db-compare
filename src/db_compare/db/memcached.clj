(ns db-compare.db.memcached
  (:import (com.danga.MemCached MemCachedClient SockIOPool))
  (:require [clj-json :as json]))

(def memcached-impl {
  :init
  (fn []
    (let [host "127.0.0.1"
          port 11211
          pool (SockIOPool/getInstance)]
      (.setServers pool (into-array [(str host ":" port)]))
      (.initialize pool)))

  :open-client
  (fn [_]
    (MemCachedClient.))

  :clear-collection
  (fn [#^MemCachedClient mcc coll]
    (.flushAll mcc))

  :insert-one
  (fn [#^MemCachedClient mcc coll record]
    (.set mcc (str coll ":" (record "id"))
              (json/generate-string record)))

  :insert-multiple
  (fn [#^MemCachedClient mcc coll records]
    (doseq [record records]
      (.set mcc (str coll ":" (record "id"))
        (json/generate-string record))))

  :get-one
  (fn [#^MemCachedClient mcc coll id]
    (if-let [s (.get mcc (str coll ":" id))]
      (json/parse-string s)))

  :get-multiple
  (fn [#^MemCachedClient mcc coll ids]
    (map json/parse-string
         (.getMultiArray mcc (into-array String (map #(str coll ":" %) ids)))))

  :update-one
  (fn [#^MemCachedClient mcc coll id attr val]
    (let [r1 (json/parse-string (.get mcc (str coll ":" id)))
          r2 (assoc r1 attr val)]
      (.set mcc (str coll ":" id) (json/generate-string r2))))

  :update-multiple
  (fn [#^MemCachedClient mcc coll ids attr val]
    (doseq [id ids]
      (let [r1 (json/parse-string (.get mcc (str coll ":" id)))
            r2 (assoc r1 attr val)]
        (.set mcc (str coll ":" id) (json/generate-string r2)))))

  :delete-one
  (fn [#^MemCachedClient mcc coll id]
    (.delete mcc (str coll ":" id)))
})
