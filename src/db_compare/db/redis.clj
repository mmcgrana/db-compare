(ns db-compare.db.redis
  (:import (org.jredis.ri.alphazero JRedisClient BulkSetMapping))
  (:require [clj-json :as json]))

(defn- init []
  {:host "127.0.0.1" :port 6379})

(defn- open-client [{:keys [#^String host #^Integer port]}]
  (JRedisClient. host port))

(defn- close-client [#^JRedisClient jredis]
  (.quit jredis))

(defn- clear [#^JRedisClient jredis]
  (.flushdb jredis))

(defn- ping [#^JRedisClient jredis]
  (.ping jredis))

(defn- insert-one [#^JRedisClient jredis record]
  (.set jredis (str (record "id")) #^String (json/generate-string record)))

(defn- insert-multiple [#^JRedisClient jredis records]
  (let [kv-set (BulkSetMapping/newStringKVSet)]
    (doseq [record records]
      (.add kv-set (str (record "id")) (json/generate-string record)))
    (.mset jredis kv-set)))

(defn- get-one [#^JRedisClient jredis id]
  (json/parse-string (String. (.get jredis (str id)))))

(defn- get-multiple [#^JRedisClient jredis ids]
  (let [id          (str (first ids))
        more-ids    (into-array String (map str (next ids)))
        bytes-array (.mget jredis id more-ids)]
    (prn (map #(json/parse-string (String. #^bytes %)) bytes-array))))

(def redis-impl
  {:name "redis"
   :init init :open-client open-client :close-client close-client
   :clear clear :ping ping
   :insert-one insert-one :insert-multiple insert-multiple
   :get-one get-one :get-multiple get-multiple})
