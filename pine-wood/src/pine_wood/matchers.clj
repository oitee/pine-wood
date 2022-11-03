(ns pine-wood.matchers
  [:require [cheshire.core :as cc]])

(def matchers
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
                              (def s* s)
                              (-> s
                                  (subs 7 (dec (count s)))
                                  ;;(clojure.string/replace  "\\\"" "\"")
                                  (cc/parse-string)
                                  (cc/generate-string)
                                  ))]])

(defn extract [line]
  (:result (reduce (fn [{:keys [text result]}
                [rgx k sanitiser-fn]]
             (let [matched-str (re-find rgx text)]
               {:text (clojure.string/replace-first text matched-str "")
                :result (assoc result k (sanitiser-fn matched-str))}))
           {:text line
            :result {}}
           matchers)))
