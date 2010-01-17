(ns db-compare.db.concurrent-hash-map
  (:import (java.util.concurrent ConcurrentHashMap)))

(def concurrent-hash-map-impl {
  :init
  (fn []
    (ConcurrentHashMap.))

  :open-client
  (fn [hm] hm)

  :clear-collection
  (fn [#^ConcurrentHashMap hm coll]
    (.clear hm))

  :insert-one
  (fn [#^ConcurrentHashMap hm coll record]
    (.put hm (str coll ":" (record "id")) record))

  :insert-multiple
  (fn [#^ConcurrentHashMap hm coll records]
    (doseq [record records]
      (.put hm (str coll ":" (record "id")) record)))

  :get-one
  (fn [#^ConcurrentHashMap hm coll id]
    (.get hm (str coll ":" id)))

  :get-multiple
  (fn [#^ConcurrentHashMap hm coll ids]
    (map #(.get hm (str coll ":" %)) ids))

  :update-one
  (fn [#^ConcurrentHashMap hm coll id attr val]
    (let [r1 (.get hm (str coll ":" id))
          r2 (assoc r1 attr val)]
      (.put hm (str coll ":" id) r2)))

  :update-multiple
  (fn [#^ConcurrentHashMap hm coll ids attr val]
    (doseq [id ids]
      (let [r1 (.get hm (str coll ":" id))
            r2 (assoc r1 attr val)]
        (.put hm (str coll ":" id) r2))))

  :delete-one
  (fn [#^ConcurrentHashMap hm coll id]
    (.remove hm (str coll ":" id)))
})
