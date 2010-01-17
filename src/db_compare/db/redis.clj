(ns db-compare.db.redis
  (:import (org.jredis.ri.alphazero JRedisClient BulkSetMapping))
  (:require [clj-json :as json]))

(def redis-impl {
  :init
  (fn []
    {:host "127.0.0.1" :port 6379})

  :open-client
  (fn  [{:keys [#^String host #^Integer port]}]
    (JRedisClient. host port))

  :close-client
  (fn [#^JRedisClient jredis]
    (.quit jredis))

  :clear-collection
  (fn [#^JRedisClient jredis coll]
    (.flushdb jredis))

  :ping
  (fn [#^JRedisClient jredis]
    (.ping jredis))

  :insert-one
  (fn [#^JRedisClient jredis coll record]
    (.set jredis (str coll ":" (record "id"))
                 #^String (json/generate-string record)))

  :insert-multiple
  (fn [#^JRedisClient jredis coll records]
    (let [kv-set (BulkSetMapping/newStringKVSet)]
      (doseq [record records]
        (.add kv-set (str coll ":" (record "id"))
                     (json/generate-string record)))
      (.mset jredis kv-set)))

  :get-one
  (fn [#^JRedisClient jredis coll id]
    (if-let [sbytes (.get jredis (str coll ":" id))]
      (json/parse-string (String. sbytes))))

  :get-multiple
  (fn [#^JRedisClient jredis coll ids]
    (let [id          (str coll ":" (first ids))
          more-ids    (into-array String (map #(str coll ":" %) (next ids)))
          bytes-array (.mget jredis id more-ids)]
      (map #(json/parse-string (String. #^bytes %)) bytes-array)))

  :update-one
  (fn [#^JRedisClient jredis coll id attr val]
    (let [r1 (json/parse-string (String. (.get jredis (str coll ":" id))))
          r2 (assoc r1 attr val)]
      (.set jredis (str coll ":" id)
                   #^String (json/generate-string r2))))

  :update-multiple
  (fn [#^JRedisClient jredis coll ids attr val]
    (let [id          (str coll ":" (first ids))
          more-ids    (into-array String (map #(str coll ":" %) (next ids)))
          bytes-array (.mget jredis id more-ids)
          r1s         (map #(json/parse-string (String. #^bytes %)) bytes-array)
          r2s         (map #(assoc % attr val) r1s)]
    (let [kv-set (BulkSetMapping/newStringKVSet)]
      (doseq [r r2s]
        (.add kv-set (str coll ":" (r "id"))
                     (json/generate-string r)))
      (.mset jredis kv-set))))

  :delete-one
  (fn [#^JRedisClient jredis coll id]
    (.del jredis (str coll ":" id)))
})
