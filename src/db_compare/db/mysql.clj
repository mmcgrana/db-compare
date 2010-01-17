(ns db-compare.db.mysql
  (:use (clojure.contrib [def :only (defvar-)])
        (db-compare.db sql)))

(defvar- mysql-custom-impl {
  :init
  (fn []
    (Class/forName "com.mysql.jdbc.Driver")
    (let [host        "127.0.0.1"
          port        3306
          username    "root"
          password    nil
          conn-string (str "jdbc:mysql://" host ":" port "/benchdb")]
      {:host host :port port :username username :password password
       :conn-string conn-string}))
})

(def mysql-impl (merge sql-impl mysql-custom-impl))
