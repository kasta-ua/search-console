#!/usr/bin/env boot

(set-env! :dependencies
  '[[org.clojure/clojure "1.10.0"]
    [modnakasta/bigq "1.1.3"]
    [org.clojure/data.csv "0.1.4"]
    [progrock "0.1.2"]])

(import
  '[java.time LocalDate]
  '[java.net URLEncoder])


(require
  '[boot.cli :refer [defclifn]]
  '[clojure.data.csv :as csv]
  '[clojure.java.io :as io]
  '[taoensso.timbre :as log]

  '[bigq.auth :as gauth]
  '[bigq.request :as req]
  '[progrock.core :as pr])

(log/merge-config! {:level :info})

(def analytics-url "https://www.googleapis.com/webmasters/v3/sites/%s/searchAnalytics/query")
(def ROW-LIMIT 5000)


;;; Request generation

(defn req-pages [date]
  {:startDate  (str date)
   :endDate    (str (.plusDays date 1))
   :searchType "web"
   :rowLimit   ROW-LIMIT
   :dimensions ["page"]})


(defn req-page-query [date target]
  {:startDate  (str date)
   :endDate    (str (.plusDays date 1))
   :searchType "web"
   :rowLimit   ROW-LIMIT
   :dimensions ["query"]
   :dimensionFilterGroups
   [{:filters [{:dimension  :page
                :expression target}]}]})


;;; Utils

(defn exec-req [token url req]
  (loop [start-row 0
         rows      nil]
    (let [res      (req/make-req token :post url
                     (assoc req :startRow start-row))
          new-rows (:rows res)
          rows     (concat rows new-rows)]
      (if (= (count new-rows) ROW-LIMIT)
        (recur (+ start-row ROW-LIMIT) rows)
        (vec rows)))))


(defn page->queries [token url date page]
  (let [page-url (-> page :keys first)]
    (or (->> (exec-req token url (req-page-query date page-url))
             (mapv #(-> %
                        (assoc :url page-url)
                        (update :keys first)))
             not-empty)
        ;; no queries found
        [(-> page
              (assoc :url page-url)
              (dissoc :keys))])))


(defn rate-limited-pmap [qps map-fn items]
  (let [bar      (atom (pr/progress-bar (count items)))
        _        (pr/print @bar)
        chunk-fn (fn [chunk]
                   (let [start    (System/currentTimeMillis)
                         res      (->> (mapv #(future (map-fn %)) chunk)
                                       (mapv deref))
                         duration (- (System/currentTimeMillis) start)
                         tosec    (- 1100 duration)]
                     (pr/print (swap! bar pr/tick qps))
                     (when (pos? tosec)
                       (Thread/sleep tosec))
                     res))
        chunks   (partition-all qps items)]
    (mapcat chunk-fn chunks)))

;;; Setup work

(defn -work! [date target token]
  (let [url     (format analytics-url
                  (URLEncoder/encode target "UTF-8"))
        pages   (exec-req token url (req-pages date))
        _       (log/infof "got %s pages for %s" (count pages) date)
        queries (rate-limited-pmap 20 #(page->queries token url date %) pages)
        ;;queries (pmap page->queries pages)
        headers [:url :keys :position :ctr :impressions :clicks]
        getter  (apply juxt headers)]

    (with-open [w (io/writer (format "data-%s.csv" date))]
      (csv/write-csv w [(map name headers)])
      (doseq [entry queries]
        (csv/write-csv w (map getter entry))))))


(defn work [date end-date target p12-path]
  (let [token    (gauth/path->token p12-path
                   ["https://www.googleapis.com/auth/webmasters.readonly"])
        date     (LocalDate/parse date)
        end-date (if end-date
                   (LocalDate/parse end-date)
                   (.plusDays date 1))]
    (loop [date date]
      (when (.isBefore date end-date)
        (-work! date target token)
        (recur (.plusDays date 1))))))

;;; Script

(defclifn -main
  "Download Search Console data to csv files"
  [t token    PATH str "path to p12 file (REQUIRED)"
   d date     DATE str "start date, like YYYY-MM-DD (REQUIRED)"
   e end-date DATE str "end date (default: download only the start date)"
   u url      URL  str "base site url (default: https://kasta.ua/)"]

  (when-not date
    (println (*usage*))
    (System/exit 0))

  (when-not token
    (println "--token is required")
    (System/exit 1))

  (work date end-date (or url "https://kasta.ua/") token))
