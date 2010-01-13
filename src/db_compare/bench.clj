(set! *warn-on-reflection* true)

(ns db-compare.bench
  (:use (clojure.contrib [def :only (defmacro- defvar-)])
        (db-compare.db noop tree-map thread-pool-server
                       fleetdb-embedded fleetdb
                       memcached redis h2 mongodb))
  (:import (java.util Random)))

(defn- nano-time []
  (System/nanoTime))

(defn timed [task]
  (let [t-start (nano-time)
        res     (task)
        t-end   (nano-time)]
    (double (/ (- t-end t-start) 1000000000))))

(defn bench [label num-queries task]
  (let [t (timed task)]
    (printf "%-36s %-8.2f %-8d\n"
      label t (int (/ num-queries t)))
    (flush)))

(defn dothreaded [num-threads #^Runnable subcoll-op]
  (let [threads (map (fn [thread-num]
                       (let [rand (Random.)]
                         (Thread. #(subcoll-op thread-num rand))))
                     (range num-threads))]
    (doseq [#^Thread thread threads] (.start thread))
    (doseq [#^Thread thread threads] (.join thread))))

(defmacro dorange [[index-name start end step] & body]
  `(doseq [~index-name (range ~start ~end ~step)]
     ~@body))

(defn record [#^Random rand id]
  {"id"        id
   "token"     (str (.nextInt rand 1000000) (.nextInt rand 1000000))
   "birthdate" (.nextInt rand 1000000000)
   "rating"    (.nextDouble rand)
   "admin"     (.nextBoolean rand)})

(defmacro- with-client [[client-name db db-impl] & body]
  `(let [~client-name ((:open-client ~db-impl) ~db)]
     (try
       ~@body
       (finally
         ((:close-client ~db-impl) ~client-name)))))

(defn- setup [db db-impl]
  (with-client [client db db-impl]
    ((:setup db-impl) client)))

(defn- clear [db db-impl]
  (with-client [client db db-impl]
    ((:clear db-impl) client)))

(defn- ping [num-pings c db db-impl]
  (let [f (:ping db-impl)]
    (dothreaded c
      (fn [thread-num _]
        (with-client [client db db-impl]
          (dotimes [_ (/ num-pings c)]
            (f client)))))))

(defn- insert-one [records c db db-impl]
  (let [f (:insert-one db-impl)]
    (dothreaded c
      (fn [thread-num _]
        (with-client [client db db-impl]
          (dorange [i thread-num (count records) c]
            (let [record (records i)]
              (f client record))))))))

(defn- insert-multiple [records insert-size c db db-impl]
  (let [f (:insert-multiple db-impl)]
    (dothreaded c
      (fn [thread-num _]
        (with-client [client db db-impl]
          (dorange [i (* thread-num insert-size) (count records) (* c insert-size)]
            (let [insert-records (subvec records i (+ i insert-size))]
              (f client insert-records))))))))

(defn- get-one-sequential [records get-one-cycles c db db-impl]
  (let [f (:get-one db-impl)]
    (dothreaded c
      (fn [thread-num _]
        (with-client [client db db-impl]
          (dotimes [_ get-one-cycles]
            (dorange [i thread-num (count records) c]
              (let [id i]
                (f client id)))))))))

(defn- get-one-random [records get-one-cycles c db db-impl]
  (let [f (:get-one db-impl)
        num-records (count records)]
    (dothreaded c
      (fn [thread-num #^Random rand]
        (with-client [client db db-impl]
          (dotimes [_ get-one-cycles]
            (dotimes [_ (/ num-records c)]
              (let [id (.nextInt rand num-records)]
                (f client id)))))))))

(defn- get-multiple-sequential [records get-multiple-size c db db-impl]
  (let [f (:get-multiple db-impl)
        num-records (count records)]
    (dothreaded c
      (fn [thread-num _]
        (with-client [client db db-impl]
          (dorange [i thread-num num-records c]
            (let [ids (map #(% "id")
                           (subvec records i (min (+ i get-multiple-size) num-records)))]
              (f client ids))))))))

(defn- get-multiple-random [records get-multiple-size c db db-impl]
  (let [f (:get-multiple db-impl)
        num-records (count records)]
    (dothreaded c
      (fn [thread-num #^Random rand]
        (with-client [client db db-impl]
          (dotimes [_ (/ num-records c)]
            (let [ids (take get-multiple-size
                        (repeatedly #(.nextInt rand num-records)))]
              (f client ids))))))))

(defn- find-one [records c db db-impl]
  (let [f (:find-one db-impl)
        num-records (count records)]
    (dothreaded c
      (fn [thread-num #^Random rand]
        (with-client [client db db-impl]
          (dotimes [_ (/ num-records c)]
            (let [birthdate ((records (.nextInt rand num-records)) "birthdate")]
              (f client birthdate))))))))

(defn- find-multiple [records find-multiple-size c db db-impl]
  (let [f (:find-multiple db-impl)
        num-records (count records)]
    (dothreaded c
      (fn [thread-num #^Random rand]
        (with-client [client db db-impl]
          (dotimes [_ (/ num-records c)]
            (let [birthdate ((records (.nextInt rand num-records)) "birthdate")]
              (f client birthdate find-multiple-size))))))))

(defn run [num-records num-pings insert-multiple-size get-one-cycles get-multiple-size find-multiple-size db-impl]
  (println "db                   " (:name db-impl))
  (println "num-records          " num-records)
  (println "num-pings            " num-pings)
  (println "insert-multiple-size " insert-multiple-size)
  (println "get-one-cycles       " get-one-cycles)
  (println "get-multiple-size    " get-multiple-size)
  (println "find-multiple-size   " find-multiple-size)
  (let [db   ((:init db-impl))
        rand    (Random.)
        records (vec (map #(record rand %) (range num-records)))]
    (when (:setup db-impl)
      (setup db db-impl))
    (dotimes [t 2]
      (doseq [c [1 10 100]]
        (println "~~~ trial" (inc t) "num-threads" c)
        (when (:ping db-impl)
          (bench "ping" num-pings
            #(ping num-pings c db db-impl)))
        (when (:insert-one db-impl)
          (clear db db-impl)
          (bench "insert-one" num-records
            #(insert-one records c db db-impl)))
        (when (:insert-multiple db-impl)
          (clear db db-impl)
          (bench "insert-multiple" (/ num-records insert-multiple-size)
            #(insert-multiple records insert-multiple-size c db db-impl)))
        (when (:get-one db-impl)
          (bench "get-one-sequential" (* num-records get-one-cycles)
            #(get-one-sequential records get-one-cycles c db db-impl))
          (bench "get-one-random" (* num-records get-one-cycles)
            #(get-one-random records get-one-cycles c db db-impl)))
        (when (:get-multiple db-impl)
          (bench "get-multiple-sequential" num-records
            #(get-multiple-sequential records get-multiple-size c db db-impl))
          (bench "get-multiple-random" num-records
            #(get-multiple-random records get-multiple-size c db db-impl)))
        (when (:find-one db-impl)
          (bench "find-one" num-records
            #(find-one records c db db-impl)))
        (when (:find-multiple db-impl)
          (bench "find-multiple" num-records
            #(find-multiple records find-multiple-size c db db-impl)))))))

(defvar- db-impls
  {:noop               noop-impl
   :tree-map           tree-map-impl
   :thread-pool-server thread-pool-server-impl
   :fleetdb-embedded   fleetdb-embedded-impl
   :fleetdb            fleetdb-impl
   :memcached          memcached-impl
   :redis              redis-impl
   :h2                 h2-impl
   :mongodb            mongodb-impl})

(let [args *command-line-args*
      db-impl              (db-impls (keyword (nth args 0)))
      num-records          (Integer. #^String (nth args 1))
      num-pings            (Integer. #^String (nth args 2))
      insert-multiple-size (Integer. #^String (nth args 3))
      get-one-cycles       (Integer. #^String (nth args 4))
      get-multiple-size    (Integer. #^String (nth args 5))
      find-multiple-size   (Integer. #^String (nth args 6))]
  (run num-records
       num-pings insert-multiple-size get-one-cycles get-multiple-size find-multiple-size
       db-impl))
