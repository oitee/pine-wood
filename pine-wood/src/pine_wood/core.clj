(ns pine-wood.core
  (:gen-class)
  (:require [cheshire.core :as cc]
            [pine-wood.matchers :as pm]))

(def counter (atom {:success 0
                    :failure 0
                    :parser-1 0
                    :parser-2 0}))

(defn parse-log-1 [line]
  (try (let [{:keys [message] :as full-log} (cc/parse-string line true)
             message (pm/extract message true)
             parsed-log (assoc full-log :message message)]
         (swap! counter assoc :success (inc (:success @counter)))
         (swap! counter assoc :parser-1 (inc (:parser-1 @counter)))
         parsed-log)
       (catch Exception e
         (when (clojure.string/starts-with? line "[20" )
           (def failed-line-from-parse-log-1* line))
         false)))


(defn parse-log-2 [line]
  (try (let [parsed-log (pm/extract line false)
             message-keys [:index
                           :total_shards
                           :search_type
                           :source
                           :took_millis
                           :total_hits]
             non-message-keys [:node.name
                               :component
                               :level
                               :timestamp]
             only-top-keys-map (select-keys parsed-log non-message-keys)
             only-message-keys-map (select-keys parsed-log message-keys)
             parsed-log (merge only-top-keys-map {:message only-message-keys-map})]
         (swap! counter assoc :success (inc (:success @counter)))
         (swap! counter assoc :parser-2 (inc (:parser-2 @counter)))
         parsed-log)
       (catch Exception e
         (when (clojure.string/starts-with? line "[20" ))
         false)))

(defn parse-log [log]
  (or (parse-log-1 log)
      (parse-log-2 log)
      (swap! counter assoc :failure (inc (:failure @counter)))))

(defn prepare-and-add-logs
  [source-dir out-path]
  (with-open [out-file (clojure.java.io/writer out-path)]
    (doseq [f (file-seq (clojure.java.io/file source-dir))]
      (when (and (.isFile f)
                 (clojure.string/includes? (.getName f) "index_search_slowlog"))
        (let [contents (slurp f)
              lines (clojure.string/split-lines contents)]
          (doseq [line lines]
            (when (seq line)
              (when-let [parsed-line (parse-log line)]
                (def one-line-parsed* parsed-line)
                (.write out-file (cc/generate-string parsed-line))
                (.write out-file "\n")))))))))



(defn add-logs [source-dir out-file]
  (reset! counter {:success 0
                   :failure 0
                   :parser-1 0
                   :parser-2 0})
  (prepare-and-add-logs source-dir out-file)
  (println @counter))
