(ns db-compare.db.ping-server
  (:import  (java.io OutputStreamWriter BufferedWriter
                     InputStreamReader BufferedReader
                     Closeable)
            (java.net Socket)))

(def ping-server-impl {
  :init
  (fn []
    {:host "127.0.0.1" :port 4444})

  :open-client
  (fn [{:keys [#^String host #^Integer port]}]
    (let [socket (Socket. host port)
          out    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket)))
          in     (BufferedReader. (InputStreamReader.  (.getInputStream  socket)))]
      {:out out :in in :sock socket}))

  :close-client
  (fn [{:keys [#^Socket sock #^Closeable out #^Closeable in]}]
    (.close sock)
    (.close out)
    (.close in))

  :ping
  (fn [{:keys [#^BufferedWriter out #^BufferedReader in]}]
    (.write out "ping\r\n")
    (.flush out)
    (.readLine in))
})
