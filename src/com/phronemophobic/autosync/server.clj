(ns com.phronemophobic.autosync.server
  (:require [tech.v3.datatype :as dt]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [clojure.core.async :as async]
            [taoensso.nippy :as nippy]
            [taoensso.tempel :as tempel]
            [clojure.java.io :as io]
            [datalevin.core :as d]
            [com.phronemophobic.tcp-pipe :as tcp-pipe]
            [com.phronemophobic.autosync.key :as key]
            [com.phronemophobic.automerge :as automerge])
  (:import [java.nio.charset StandardCharsets]
           [java.net InetSocketAddress Socket StandardSocketOptions ServerSocket]
           java.io.File
           java.io.DataOutputStream
           java.io.DataInputStream))

(defn ^:private ->native-buffer [^bytes bs]
  (let [buf (native-buffer/malloc (alength bs))]
    (dt/copy! bs buf)
    buf))


(def peer-state-table "peer-state-table")
(def db (delay
          (doto (d/open-kv "syncstate.db")
            (d/open-dbi peer-state-table))))



(defn load-doc [doc-id]
  (let [local-save (d/get-value @db peer-state-table
                                doc-id)
        local-doc (if local-save
                    (automerge/load (->native-buffer local-save))
                    (automerge/doc))]
    local-doc))

(defn sync-handler [socket key]
  (let [write-ch (async/chan 10)
        read-ch (async/chan 10)]
    (tcp-pipe/handle-io socket key write-ch read-ch)
    (loop [state {}]
      (when-let [msg (async/<!! read-ch)]
        (tap> {:msg msg
               :state state})
        (case (:op msg)
          
          :sync
          (let [{:keys [doc-id sync-message]} msg
                sync-message (->native-buffer sync-message)
                
                local-doc (if-let [local-doc (get-in state [doc-id :local-doc])]
                            local-doc
                            (load-doc doc-id))
                sync-state (if-let [sync-state (get-in state [doc-id :sync-state])]
                             sync-state
                             (automerge/sync-state-init))
                
                _ (automerge/receive-sync-message local-doc sync-state sync-message)
                
                response-sync-message (automerge/generate-sync-message local-doc sync-state)
                response-message (if response-sync-message
                                   {:op :sync
                                    :doc-id doc-id
                                    :sync-message (dt/->byte-array response-sync-message)}
                                   {:op :sync-complete 
                                    :doc-id doc-id})
                _ (async/>!! write-ch response-message)
                state (-> state
                          (assoc-in [doc-id :sync-state] sync-state)
                          (assoc-in [doc-id :local-doc] local-doc))]
            (d/transact-kv @db
                           [[:put peer-state-table doc-id 
                             (-> local-doc automerge/save dt/->byte-array)]])
            (recur state))
          
          :sync-complete nil)))))

(comment
  

  (def server
    (tcp-pipe/start-server
     3002
     (fn [socket]
       (future
         (try
           (sync-handler socket)
           (catch Exception e
             (tap> e)
             (prn e))
           (finally
             (.close socket)
             (tap> :socket-closing)))))))

  (.close server)

  
  ,)


(comment
  (save-key "foo.key" (byte-array (range 255)))
  (load-key "foo.key")
  
  ,)

(defn -main [port key-file]
  (let [key (key/load-key key-file)
        port (parse-long port)]
    (tcp-pipe/start-server
     port
     (fn [^Socket socket]
       (future
         (try
           (sync-handler socket key)
           (catch Exception e
             (tap> e)
             (prn e))
           (finally
             (.close socket)
             (tap> :socket-closing))))))
    (println "server started...")
    ;; wait indefinitely
    @(promise)))