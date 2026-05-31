# autosync

An opinioned server and client for syncing automerge documents.


## Usage

### Generate a key

$ clojure -M -m com.phronemophobic.autosync.key 'my.key'
key saved to "my.key"

### Start an autosync server

$ clojure -M -m com.phronemophobic.autosync.server 5001 my.key 
server started...

### Sync your automerge documents


```clojure

(require '[com.phronemophobic.autosync.client :as autosync.client]
         '[com.phronemophobic.autosync.key :as autosync.key]
         '[com.phronemophobic.automerge :as automerge])


(def mykey (autosync.key/load-key "my.key"))

(def mydoc (automerge/doc))
(automerge/put! mydoc "hello" "world")

(def host ...)
(autosync.client/sync-doc! {:host host :port 5001 :key mykey}
                           "mydoc"
                           mydoc)

```


## License

Copyright © 2026 Adrian

Licensed under Apache License v2.0.
