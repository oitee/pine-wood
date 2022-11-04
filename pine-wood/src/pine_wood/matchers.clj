(ns pine-wood.matchers
  [:require [cheshire.core :as cc]]
  (:import java.time.LocalDateTime
           java.time.format.DateTimeFormatter))

(defn convert-timestamp [t]
  (str (.format (LocalDateTime/parse t (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss,SSS"))
          (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss,SSS")) "+0000"))

(def matchers-1
  [[#"\[[\w-]+\]" :index (fn [s] (subs s 1 (dec (count s))))]
   [#"took_millis\[[0-9]+\]" :took_millis (fn [s]
                                            (Integer/parseInt (subs s 12 (dec (count s)))))]
   [#"total_hits\[[0-9+]+ hits\]" :total_hits (fn [s]
                                               (subs s 11 (- (count s) 6)))]
   [#"search_type\[[\w_]+\]" :search_type (fn [s]
                                            (subs s 12 (dec (count s))))]
   [#"total_shards\[[0-9]+\]" :total_shards (fn [s]
                                              (Integer/parseInt (subs s 13 (dec (count s)))))]
   ;; Cheeky hack: we must extract id first for source to be findable
   [#"id\[[\w]*\]" :id identity]
   [#"source\[.*\]" :source (fn [s]
                              (-> s
                                  (subs 7 (dec (count s)))
                                  ;;(clojure.string/replace  "\\\"" "\"")
                                  (cc/parse-string)
                                  (cc/generate-string)))]])
(def matchers-2
  [[#"\[[^\]]+\]"
    :timestamp
    (fn [s] (convert-timestamp (subs s 1 (dec (count s)))))]
   [#"\[[\w]+ ]" :level (fn [s] (subs s 1 (- (count s) 2)))]
   [#"\[[\w.]+   " :component (fn [s]
                                (as-> s s*
                                  (subs s* 1 (dec (count s*)))
                                  (clojure.string/trim s*)))]
   [#" \[[\w+0-9-]+\] " :node.name (fn [s]
                                     (subs s 2 (- (count s) 2)))]
   [#"\[[\w0-9-]+\]" :index (fn [s]
                              (subs s 1 (dec (count s))))]
   [#"took_millis\[[0-9]+\]" :took_millis (fn [s]
                                            (Integer/parseInt (subs s 12 (dec (count s)))))]
   [#"total_hits\[[0-9+]+ hits\]" :total_hits (fn [s]
                                                (subs s 11 (- (count s) 6)))]
   [#"search_type\[[\w_]+\]" :search_type (fn [s]
                                            (subs s 12 (dec (count s))))]
   [#"total_shards\[[0-9]+\]" :total_shards (fn [s]
                                              (Integer/parseInt (subs s 13 (dec (count s)))))]
   [#"source\[.*\]" :source (fn [s]
                              (-> s
                                  (subs 7 (dec (count s)))
                                  ;;(clojure.string/replace  "\\\"" "\"")
                                  (cc/parse-string)
                                  (cc/generate-string)))]])
(defn extract [line first-try?]
  (let [matchers (if first-try?
                   matchers-1
                   matchers-2)]
    (:result (reduce (fn [{:keys [text result]}
                          [rgx k sanitiser-fn]]
                       (let [matched-str (re-find rgx text)]
                         {:text (clojure.string/replace-first text matched-str "")
                          :result (assoc result k (sanitiser-fn matched-str))}))
                     {:text line
                      :result {}}
                     matchers))))
