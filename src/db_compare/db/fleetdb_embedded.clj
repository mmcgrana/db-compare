(ns db-compare.db.fleetdb-embedded
  (:require (fleetdb [embedded :as fe])))

(defn- init []
  (fe/init-persistent "/tmp/fleetdb-embedded-bench.fdb"))

(defn- open-client [dba]
  dba)

(defn- close-client [dba])

(defn- setup [dba]
  (fe/query dba
    ["create-index" "records" "birthdate"]))

(defn- clear [dba]
  (fe/query dba
    ["delete" "records"]))

(defn- insert-one [dba record]
  (fe/query dba
    ["insert" "records" record]))

(defn- insert-multiple [dba records]
  (fe/query dba
    ["insert" "records" records]))

(defn- get-one [dba id]
  (first (fe/query dba
           ["select" "records" {"where" ["=" "id" id]}])))

(defn- get-multiple [dba ids]
  (dorun (fe/query dba
           ["select" "records" {"where" ["in" "id" ids]}])))

(defn- find-one [dba birthdate]
  (first (fe/query dba
           ["select" "records" {"where" ["=" "birthdate" birthdate]}])))

(defn- find-multiple [dba birthdate find-multiple-size]
  (dorun (fe/query dba
           ["select" "records"
             {"where" [">=" "birthdate" birthdate]
              "limit" find-multiple-size}])))

(defn- find-filtered [dba birthdate rating find-filtered-size]
  (dorun (fe/query dba
           ["select" "records"
             {"where" ["and" [">=" "birthdate" birthdate]
                             [">" "rating" rating]]
              "limit" find-filtered-size}])))

(defn- update-one [dba id rating]
  (fe/query dba
    ["update" "records" {"rating" rating} {"where" ["=" "id" id]}]))

(defn- update-multiple [dba ids rating]
  (fe/query dba
    ["update" "records" {"rating" rating} {"where" ["in" "id" ids]}]))

(def fleetdb-embedded-impl
  {:name "fleetdb-embedded"
   :init init :open-client open-client :close-client close-client
   :setup setup :clear clear
   :insert-one insert-one :insert-multiple insert-multiple
   :get-one get-one :get-multiple get-multiple
   :find-one find-one :find-multiple find-multiple :find-filtered find-filtered
   :update-one update-one :update-multiple update-multiple})
