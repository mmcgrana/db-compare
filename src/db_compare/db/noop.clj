(ns db-compare.db.noop)

(defn- init [])

(defn- open-client [_])

(defn- close-client [_])

(defn- setup [_])

(defn- clear [_])

(defn- insert-one [_ record]
  (identity record))

(defn- insert-multiple [_ records]
  (dorun records))

(defn- get-one [_ id]
  (identity id))

(defn- get-multiple [_ ids]
  (dorun ids))

(defn- find-one [_ birthdate]
  (identity birthdate))

(defn- find-multiple [_ birthdate find-multiple-size]
  (identity birthdate))

(def noop-impl
  {:name "noop"
   :init init :open-client open-client :close-client close-client
   :setup setup :clear clear
   :insert-one insert-one :insert-multiple insert-multiple
   :get-one get-one :get-multiple get-multiple
   :find-one find-one :find-multiple find-multiple})

