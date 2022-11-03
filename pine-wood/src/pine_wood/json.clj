(ns pine-wood.json
  (:require [cheshire.core :as cc]
            [pine-wood.matchers :as pm]))

(def counter (atom {:success 0
                    :failure 0}))

(defn do-stuff
  [source-file out-path]
  (with-open [out-file (clojure.java.io/writer out-path)]
    (doseq [f (file-seq (clojure.java.io/file source-file))]
      
      (when (.isFile f)
        (let [contents (slurp f)
              lines (clojure.string/split-lines contents)]
          (doseq [line lines]
            (when (seq line)
              (when-let [parsed-line (parse-log line)]
                (def one-line* line)
                (.write out-file (cc/generate-string parsed-line))
                (.write out-file "\n")))))))))

(defn parse-log [line]
  (try (let [{:keys [message] :as full-log} (cc/parse-string line true)
             parsed-log (-> full-log
                            (update :message pm/extract)
                            ;;(assoc :raw_log line)
                            (assoc :error_log "false"))]
     (swap! counter assoc :success (inc (:success @counter)))
     parsed-log)
       (catch Exception e
         (swap! counter assoc :failure (inc (:failure @counter)))
         (println "Error.")
         {;;:raw_log line
          :error_log "true"})))

(defn add-logs [source-file out-file]
  (reset! counter {:success 0
                   :failure 0})
  (do-stuff source-file out-file)
  (println @counter))
