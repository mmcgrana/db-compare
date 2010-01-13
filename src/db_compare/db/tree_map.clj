(ns db-compare.db.tree-map
  (:import (java.util TreeMap)))

(defn- init []
  (TreeMap.))

(defn- open-client [tm]
  tm)

(defn- close-client [tm])

(defn- clear [#^TreeMap tm]
  (.clear tm))

(defn- insert-one [#^TreeMap tm record]
  (locking tm
    (.put tm (record "id") record)))

(defn- get-one [#^TreeMap tm id]
  (.get tm id))

(def tree-map-impl
  {:name "tree-map"
   :init init :open-client open-client :close-client close-client :clear clear
   :insert-one insert-one :get-one get-one})
