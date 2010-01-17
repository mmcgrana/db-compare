(let [mag 1000
      cnk 20]
  {:db-type               :mongodb
   :num-trials            2
   :concurrencies         [1 10]
   :num-records           mag
   :num-ping              mag
   :size-insert-multiple  cnk
   :num-get-one           mag
   :num-get-multiple      mag
   :size-get-multiple     cnk
   :num-find-one          mag
   :num-find-above        mag
   :size-find-above       cnk
   :num-update-one        mag
   :num-update-multiple   mag
   :size-update-multiple  cnk
   :num-mixed             mag})
