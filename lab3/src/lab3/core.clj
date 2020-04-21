  
(ns lab3.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))


(defn saveSeq [name format]
  (cond
    (= format "tsv") (with-open [reader (io/reader name)]
                       (doall
                        (csv/read-csv reader :separator \tab)))
    (= format "csv") (with-open [reader (io/reader name)]
                       (doall
                        (csv/read-csv reader)))))


(defn data->maps [head & lines]
  (map #(zipmap (map keyword head) %1) lines))



(defn makeTable [name format]
  (apply data->maps (saveSeq name format)))

(defn printTable [table]
  (loop [k 0]
    (when (< k (count (first table)))
      (print (format "%40s| " (name (nth (keys (first table)) k))))
      (recur (+ k 1))))
  (println)
  (loop [i 0]
    (when (< i (count table))
      (loop [j 0]
        (when (< j (count (nth table i)))
          (print (format "%40s| " (nth (vals (nth table i)) j)))
          (recur (+ j 1))))
      (println "")
      (recur (+ i 1)))))



(defn -main
  [& args]
  (println "Input name of file: ")
  (def input (read-line))
  (def formatFile (str (nth (str/split input #"\.") 1)))
  (cond
    (= formatFile "csv") (printTable (makeTable input formatFile))
    (= formatFile "tsv") (printTable (makeTable input formatFile))
    :else "Error: unacceptable file format"))