(ns db-compare.db.fleetdb-embedded
  (:require (fleetdb [embedded :as fe])))

(def fleetdb-embedded-impl {
  :init
  (fn []
    (fe/init-persistent "/tmp/fleetdb-embedded-bench.fdb"))

  :open-client
  (fn [dba]
    dba)

  :clear-collection
  (fn [dba coll]
    (fe/query dba ["delete" coll]))

  :ensure-index
  (fn [dba coll attr]
    (fe/query dba ["create-index" coll attr]))

  :insert-one
  (fn [dba coll record]
    (fe/query dba ["insert" coll record]))

  :insert-multiple
  (fn [dba coll records]
    (fe/query dba ["insert" coll records]))

  :get-one
  (fn [dba coll id]
    (first (fe/query dba ["select" coll {"where" ["=" "id" id]}])))

  :get-multiple
  (fn [dba coll ids]
    (fe/query dba ["select" coll {"where" ["in" "id" ids]}]))

  :find-one
  (fn [dba coll attr val]
    (first (fe/query dba ["select" coll {"where" ["=" attr val]}])))

  :find-above
  (fn [dba coll attr val limit]
    (fe/query dba ["select" coll {"where" [">" attr val] "limit" limit}]))

  :find-above2
  (fn [dba coll attr1 val1 attr2 val2 limit]
    (fe/query dba ["select" coll {"where" ["and" [">" attr1 val1]
                                                 [">" attr2 val2]]
                                  "limit" limit}]))

  :update-one
  (fn [dba coll id attr val]
    (fe/query dba ["update" coll {attr val} {"where" ["=" "id" id]}]))

  :update-multiple
  (fn [dba coll ids attr val]
    (fe/query dba ["update" coll {attr val} {"where" ["in" "id" ids]}]))

  :delete-one
  (fn [dba coll id]
    (fe/query dba ["delete" coll {"where" ["=" "id" id]}]))
})
