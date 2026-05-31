(ns com.phronemophobic.autosync.client
  (:require [clojure.core.async :as async]
            [com.phronemophobic.tcp-pipe :as tcp-pipe]
            [com.phronemophobic.automerge :as automerge]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype :as dt]))


(defn ^:private ->native-buffer [bs]
  (let [buf (native-buffer/malloc (alength bs))]
    (dt/copy! bs buf)
    buf))


(defn sync-doc! [{:keys [host port key]} doc-id doc]

  (let [read-ch (async/chan 10)
        write-ch (async/chan 10)
        sync-state (automerge/sync-state-init)]
    (with-open [client (tcp-pipe/start-client host port key write-ch read-ch)]
      (loop []
        (let [_ (automerge/commit! doc)
              sync-message (automerge/generate-sync-message doc sync-state)

              message (if sync-message
                        {:op :sync
                         :doc-id doc-id
                         :sync-message (dt/->byte-array sync-message)}
                        {:op :sync-complete
                         :doc-id doc-id})
              
              _ (async/>!! write-ch message)
              response (async/<!! read-ch)]
          (when (= :sync (:op response))
            (let [sync-message (->native-buffer
                                (:sync-message response))]
              (automerge/receive-sync-message doc sync-state sync-message)
              (recur))))))
    
    ;; return sync stats or something
    nil
    ))