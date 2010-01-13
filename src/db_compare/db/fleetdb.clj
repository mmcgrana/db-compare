(ns db-compare.db.fleetdb
  (:require (fleetdb [client :as fc])))

(defn- init []
  {:host "127.0.0.1" :port 3400})

(defn- open-client [db]
  (fc/connect db))

(defn- close-client [client]
  (fc/close client))

(defn- setup [client]
  (client
    ["create-index" "records" "birthdate"]))

(defn- clear [client]
  (client
    ["delete" "records"]))

(defn- ping [client]
  (client
    ["ping"]))

(defn- insert-one [client record]
  (client
    ["insert" "records" record]))

(defn- insert-multiple [client records]
  (client
    ["insert" "records" records]))

(defn- get-one [client id]
  (first (client
           ["select" "records" {"where" ["=" "id" id]}])))

(defn- get-multiple [client ids]
  (client
    ["select" "records" {"where" ["in" "id" ids]}]))

(defn- find-one [client birthdate]
  (first (client
           ["select" "records" {"where" ["=" "birthdate" birthdate]}])))

(defn- find-multiple [client birthdate find-multiple-size]
  (client
    ["select" "records"
      {"where" [">=" "birthdate" birthdate]
       "limit" find-multiple-size}]))

(def fleetdb-impl
  {:name "fleetdb"
   :init init :open-client open-client :close-client close-client
   :setup setup :clear clear :ping ping
   :insert-one insert-one :insert-multiple insert-multiple
   :get-one get-one :get-multiple get-multiple
   :find-one find-one :find-multiple find-multiple})
