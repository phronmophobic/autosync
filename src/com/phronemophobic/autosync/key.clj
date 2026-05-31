(ns com.phronemophobic.autosync.key
  (:require [taoensso.tempel :as tempel]
            [clojure.java.io :as io])
  (:import
   java.io.ByteArrayInputStream
   java.io.ByteArrayOutputStream))

(defn save-key [f key]
  (with-open [os (io/output-stream f)
              bais (ByteArrayInputStream. key)]
    (io/copy bais
             os)))
(defn load-key [f]
  (let [f (io/file f)
        size (.length f)]
    (with-open [is (io/input-stream f)
                baos (ByteArrayOutputStream. size)]
      (io/copy is baos)
      (.toByteArray baos))))

(defn -main
  ([fname]
   (-main fname nil))
  ([fname key-size]
   (let [key-size (or key-size 32)]
     (save-key fname (tempel/rand-ba key-size))
     (println (str "key saved to \"" fname  "\".")))))