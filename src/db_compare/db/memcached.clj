(ns db-compare.db.memcached
  (:import (com.danga.MemCached MemCachedClient SockIOPool))
  (:require [clj-json :as json]))

(defn- init []
  (let [pool (SockIOPool/getInstance)]
  (.setServers pool (into-array ["localhost:11211"]))
  (.initialize pool)))

(defn- open-client [_]
  (MemCachedClient.))

(defn- close-client [#^MemCachedClient mcc])

(defn- clear [#^MemCachedClient mcc]
  (.flushAll mcc))

(defn- insert-one [#^MemCachedClient mcc record]
  (.set mcc (str (record "id")) (json/generate-string record)))

(defn- get-one [#^MemCachedClient mcc id]
  (json/parse-string (.get mcc (str id))))

(defn- get-multiple [#^MemCachedClient mcc ids]
  (map json/parse-string
       (.getMultiArray mcc (into-array String (map str ids)))))

(def memcached-impl
  {:name "memcached"
   :init init :open-client open-client :close-client close-client
   :clear clear :insert-one insert-one
   :get-one get-one :get-multiple get-multiple})
