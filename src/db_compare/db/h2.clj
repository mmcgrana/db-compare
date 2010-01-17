(ns db-compare.db.h2
  (:use (clojure.contrib [def :only (defvar-)])
        (db-compare.db sql)))

(defvar- h2-custom-impl {
  :init
  (fn []
    (Class/forName "org.h2.Driver")
    (let [host        "127.0.0.1"
          username    "sa"
          password    nil
          conn-string (str "jdbc:h2:tcp://" host "/benchdb;MULTI_THREADED=1")]
      {:host host :conn-string conn-string}))
})

(def h2-impl (merge sql-impl h2-custom-impl))
