(ns db-compare.db.fleetdb-server
  (:require (fleetdb [client :as fc])))

(def fleetdb-server-impl {
  :init
  (fn []
    {:host "127.0.0.1" :port 3400})

  :open-client
  (fn [db]
    (fc/connect db))

  :close-client
  (fn [client]
    (fc/close client))

  :clear-collection
  (fn [client coll]
    (client ["delete" coll]))

  :ensure-index
  (fn [client coll attr]
    (client ["create-index" coll attr]))

  :ping
  (fn [client]
    (client ["ping"]))

  :insert-one
  (fn [client coll record]
    (client ["insert" coll record]))

  :insert-multiple
  (fn [client coll records]
    (client ["insert" coll records]))

  :get-one
  (fn [client coll id]
    (first (client ["select" coll {"where" ["=" "id" id]}])))

  :get-multiple
  (fn [client coll ids]
    (client ["select" coll {"where" ["in" "id" ids]}]))

  :find-one
  (fn [client coll attr value]
    (first (client ["select" coll {"where" ["=" attr value]}])))

  :find-above
  (fn [client coll attr val limit]
    (client ["select" coll {"where" [">" attr val] "limit" limit}]))

  :find-above2
  (fn [client coll attr1 val1 attr2 val2 limit]
    (client ["select" coll {"where" ["and" [">" attr1 val1]
                                           [">" attr2 val2]]
                            "limit" limit}]))

  :update-one
  (fn [client coll id attr val]
    (client ["update" coll {attr val} {"where" ["=" "id" id]}]))

  :update-multiple
  (fn [client coll ids attr val]
    (client ["update" coll {attr val} {"where" ["in" "id" ids]}]))

  :delete-one
  (fn [client coll id]
    (client ["delete" coll {"where" ["=" "id" id]}]))
})
