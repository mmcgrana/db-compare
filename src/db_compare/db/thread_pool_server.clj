(ns db-compare.db.thread-pool-server
  (:import  (java.io OutputStreamWriter BufferedWriter
                     InputStreamReader BufferedReader
                     Closeable)
            (java.net Socket)))

(defn- init []
  {:host "127.0.0.1" :port 4444})

(defn- open-client [{:keys [host port]}]
  (let [socket (Socket. "127.0.0.1" 4444)
        out    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket)))
        in     (BufferedReader. (InputStreamReader.  (.getInputStream  socket)))]
    {:out out :in in :sock socket}))

(defn- close-client [{:keys [#^Socket sock #^Closeable out #^Closeable in]}]
  (.close sock)
  (.close out)
  (.close in))

(defn- ping [{:keys [#^BufferedWriter out #^BufferedReader in]}]
  (.write out "ping\r\n")
  (.flush out)
  (.readLine in))

(def thread-pool-server-impl
  {:name "thread-pool-server"
   :init init :open-client open-client :close-client close-client
   :ping ping})
