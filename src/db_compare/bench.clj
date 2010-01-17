(set! *warn-on-reflection* true)

(ns db-compare.bench
  (:use (clojure.contrib [def :only (defmacro- defvar-)])
        (clojure.contrib [str-utils :only (re-split)])
        (db-compare.db concurrent-hash-map fleetdb-embedded fleetdb-server
                       h2 memcached mongodb mysql null-store ping-server
                       postgresql redis))
  (:import (java.util Random)))

(defn- record [#^Random rand id]
  {"id"        id
   "token"     (str (.nextInt rand 1000000) (.nextInt rand 1000000))
   "birthdate" (.nextInt rand 1000000000)
   "rating"    (.nextDouble rand)
   "admin"     (.nextBoolean rand)})

(defn- nano-time []
  (System/nanoTime))

(defn- timed [task]
  (let [t-start (nano-time)
        res     (task)
        t-end   (nano-time)]
    (double (/ (- t-end t-start) 1000000000))))

(defn- dothreaded [num-threads #^Runnable subcoll-op]
  (let [threads (map (fn [thread-num]
                       (let [rand (Random.)]
                         (Thread. #(subcoll-op thread-num rand))))
                     (range num-threads))]
    (doseq [#^Thread thread threads] (.start thread))
    (doseq [#^Thread thread threads] (.join thread))))

(defmacro- dorange [[index-name [start end step]] & body]
  `(doseq [~index-name (range ~start ~end ~step)]
     ~@body))

(defn- random-ids [#^Random rand n k]
  (seq
    (loop [ids #{}]
      (if (= k (count ids))
        ids
        (recur (conj ids (.nextInt rand n)))))))

(defmacro- with-client [[client-name db db-impl] & body]
  `(let [~client-name ((:open-client ~db-impl) ~db)]
     (try
       ~@body
       (finally
         (if-let [closer# (:close-client ~db-impl)]
           (closer# ~client-name))))))

(defmacro dothreaded-client [[[client-sym cnum-sym rand-sym] [db db-impl conc]] body]
  `(dothreaded ~conc
     (fn [~cnum-sym ~rand-sym]
       (with-client [~client-sym ~db ~db-impl]
         ~body))))

(defmacro- report [label num-queries body]
  `(let [t# (timed (fn [] ~body))]
     (printf "%-36s %-8.2f %-8d\n" ~label t# (int (/ ~num-queries t#)))
     (flush)))

(defmacro- report-if-let [bind-form label num-queries body]
  `(if-let ~bind-form
     (report ~label ~num-queries ~body)))

(defn- ensure-collection [db db-impl]
  (if-let [f (:ensure-collection db-impl)]
    (with-client [client db db-impl]
      (f client "records"))))

(defn- ensure-index [db db-impl]
  (when-let [f (:ensure-index db-impl)]
    (with-client [client db db-impl]
      (f client "records" "birthdate"))))

(defn- clear-collection [db db-impl]
  (if-let [f (:clear-collection db-impl)]
    (with-client [client db db-impl]
      (f client "records"))))

(defn- ping [db db-impl conc num-pings]
  (report-if-let [f (:ping db-impl)] "ping" num-pings
    (let [num-pings-per (/ num-pings conc)]
      (dothreaded-client [[client _ _] [db db-impl conc]]
        (dotimes [_ num-pings-per]
          (f client))))))

(defn- insert-one [db db-impl records num-records conc]
  (when-let [f (:insert-one db-impl)]
    (clear-collection db db-impl)
    (report "insert-one" num-records
      (dothreaded-client [[client cnum _] [db db-impl conc]]
        (dorange [i [cnum num-records conc]]
          (let [record (records i)]
            (f client "records" record)))))))

(defn- insert-multiple [db db-impl records num-records conc size-insert-multiple]
  (when-let [f (:insert-multiple db-impl)]
    (clear-collection db db-impl)
    (report "insert-multiple" (/ num-records size-insert-multiple)
      (dothreaded-client [[client cnum _] [db db-impl conc]]
        (dorange [i [(* cnum size-insert-multiple) num-records (* conc size-insert-multiple)]]
          (let [insert-records (subvec records i (+ i size-insert-multiple))]
            (f client "records" insert-records)))))))

(defn- get-one-sequential [db db-impl records num-records conc num-get-one]
  (report-if-let [f (:get-one db-impl)] "get-one" num-get-one
    (let [num-gets-per (/ num-get-one conc)]
      (dothreaded-client [[client cnum rand] [db db-impl conc]]
        (dotimes [i num-gets-per]
          (let [id (rem (+ cnum (* i conc)) num-records)]
            (f client "records" id)))))))

(defn- get-one-random [db db-impl records num-records conc num-get-one]
  (report-if-let [f (:get-one db-impl)] "get-one" num-get-one
    (let [num-gets-per (/ num-get-one conc)]
      (dothreaded-client [[client cnum rand] [db db-impl conc]]
        (dotimes [_ num-gets-per]
          (let [id (.nextInt #^Random rand num-records)]
            (f client "records" id)))))))

(defn- get-multiple-sequential [db db-impl records num-records conc num-get-multiple size-get-multiple]
  (report-if-let [f (:get-multiple db-impl)] "get-multiple-sequential" num-get-multiple
    (let [num-gets-per (/ num-get-multiple conc)]
      (dothreaded-client [[client cnum _] [db db-impl conc]]
        (dotimes [i num-gets-per]
          (let [start (+ cnum (* i conc))
                ids (map #(rem % num-records) (range start (+ start size-get-multiple)))]
            (f client "records" ids)))))))

(defn- get-multiple-random [db db-impl records num-records conc num-get-multiple size-get-multiple]
  (report-if-let [f (:get-multiple db-impl)] "get-multiple" num-get-multiple
    (let [num-gets-per (/ num-get-multiple conc)]
      (dothreaded-client [[client cnum rand] [db db-impl conc]]
        (dotimes [i num-gets-per]
          (let [ids (random-ids rand num-records size-get-multiple)]
            (f client "records" ids)))))))

(defn- find-one [db db-impl records num-records conc num-find-one]
  (report-if-let [f (:find-one db-impl)] "find-one" num-find-one
    (let [num-finds-per (/ num-find-one conc)]
      (dothreaded-client [[client cnum #^Random rand] [db db-impl conc]]
        (dotimes [_ num-finds-per]
          (let [birthdate ((records (.nextInt rand num-records)) "birthdate")]
            (f client "records" "birthdate" birthdate)))))))

(defn- find-above [db db-impl records num-records conc num-find-above size-find-above]
  (report-if-let [f (:find-above db-impl)] "find-above" num-find-above
    (let [num-finds-per (/ num-find-above conc)]
      (dothreaded-client [[client cnum #^Random rand] [db db-impl conc]]
        (dotimes [_ num-finds-per]
          (let [birthdate ((records (.nextInt rand num-records)) "birthdate")]
            (f client "records" "birthdate" birthdate size-find-above)))))))

(defn- find-above2 [db db-impl records num-records conc num-find-above size-find-above]
  (report-if-let [f (:find-above2 db-impl)] "find-above2" num-find-above
    (let [num-finds-per (/ num-find-above conc)]
      (dothreaded-client [[client cnum #^Random rand] [db db-impl conc]]
        (dotimes [_ num-finds-per]
          (let [birthdate ((records (.nextInt rand num-records)) "birthdate")]
            (f client "records" "birthdate" birthdate "rating" 0.8 size-find-above)))))))

(defn- update-one [db db-impl records num-records conc num-update-one]
  (report-if-let [f (:update-one db-impl)] "update-one" num-update-one
    (let [num-updates-per (/ num-update-one conc)]
      (dothreaded-client [[client cnum #^Random rand] [db db-impl conc]]
        (dotimes [_ num-updates-per]
          (let [id     (.nextInt rand num-records)
                rating (.nextDouble rand)]
            (f client "records" id "rating" rating)))))))

(defn- update-multiple [db db-impl records num-records conc num-update-multiple size-update-multiple]
  (report-if-let [f (:update-multiple db-impl)] "update-multiple" num-update-multiple
    (let [num-updates-per (/ num-update-multiple conc)]
      (dothreaded-client [[client cnum #^Random rand] [db db-impl conc]]
        (dotimes [_ num-updates-per]
          (let [ids    (random-ids rand num-records size-update-multiple)
                rating (.nextDouble rand)]
            (f client "records" ids "rating" rating)))))))

(defn- mixed [db db-impl records num-records conc num-mixed]
  (let [g (:get-one db-impl)
        d (:delete-one db-impl)
        i (:insert-one db-impl)]
    (if (and g d i)
      (let [num-mixed-sets-per (/ num-mixed conc 10)]
        (report "mixed" num-mixed
          (dothreaded-client [[client cnum #^Random rand] [db db-impl conc]]
            (dotimes [_ num-mixed-sets-per]
              (dotimes [_ 8]
                (let [id (.nextInt rand num-records)]
                  (g client "records" id)))
              (let [id  (.nextInt rand num-records)
                    rec (record rand id)]
                (d client "records" id)
                (i client "records" rec)))))))))

(defn db-impl-for [db-type]
  (db-type
     {:concurrent-hash-map concurrent-hash-map-impl
      :fleetdb-embedded    fleetdb-embedded-impl
      :fleetdb-server      fleetdb-server-impl
      :fleetdb-embedded    fleetdb-embedded-impl
      :h2                  h2-impl
      :memcached           memcached-impl
      :mongodb             mongodb-impl
      :mysql               mysql-impl
      :null-store          null-store-impl
      :ping-server         ping-server-impl
      :postgresql          postgresql-impl
      :redis               redis-impl}))

(defn run [{:keys [db-type num-trials concurrencies num-records num-ping
                   size-insert-multiple num-get-one num-get-multiple
                   size-get-multiple num-find-one num-find-above
                   size-find-above num-update-one num-update-multiple
                   size-update-multiple num-mixed]}]
  (let [db-impl (db-impl-for db-type)
        db      ((:init db-impl))
        records (let [rand (Random.)]
                  (vec (map #(record rand %) (range num-records))))]
    (ensure-collection db db-impl)
    (ensure-index db db-impl)
    (dotimes [trial num-trials]
      (doseq [conc concurrencies]
        (println "\n~~~ trial" (inc trial) "conc" conc)
        (ping                    db db-impl                     conc num-ping)
        (insert-one              db db-impl records num-records conc)
        (insert-multiple         db db-impl records num-records conc size-insert-multiple)
        (get-one-sequential      db db-impl records num-records conc num-get-one)
        (get-one-random          db db-impl records num-records conc num-get-one)
        (get-multiple-sequential db db-impl records num-records conc num-get-multiple size-get-multiple)
        (get-multiple-random     db db-impl records num-records conc num-get-multiple size-get-multiple)
        (find-one                db db-impl records num-records conc num-find-one)
        (find-above              db db-impl records num-records conc num-find-above size-find-above)
        (find-above2             db db-impl records num-records conc num-find-above size-find-above)
        (update-one              db db-impl records num-records conc num-update-one)
        (update-multiple         db db-impl records num-records conc num-update-multiple size-update-multiple)
        (mixed                   db db-impl records num-records conc num-mixed)))))

(run (-> (first *command-line-args*) slurp read-string eval))
