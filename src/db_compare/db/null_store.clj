(ns db-compare.db.null-store)

(def null-store-impl {
  :init
  (fn [])

  :open-client
  (fn [db])

  :close-client
  (fn [client])

  :clear-collection
  (fn [client coll])

  :ensure-index
  (fn [client coll attr])

  :ping
  (fn [client])

  :insert-one
  (fn [client coll record])

  :insert-multiple
  (fn [client coll records])

  :get-one
  (fn [client coll id])

  :get-multiple
  (fn [client coll ids])

  :find-one
  (fn [client coll attr value])

  :find-above
  (fn [client coll attr val limit])

  :find-above2
  (fn [client coll attr1 val1 attr2 val2 limit])

  :update-one
  (fn [client coll id attr val])

  :update-multiple
  (fn [client coll ids attr val])

  :delete-one
  (fn [client coll id])
})

