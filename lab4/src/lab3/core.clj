  
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
  (into [] (apply data->maps (saveSeq name format))))

(defn printTable [table]
  (loop [k 0]
    (when (< k (count (first table)))
      (print (format "|%-40s| " (name (first (keys (nth (first table) k))))))
      (recur (+ k 1))))
  (println)
  (loop [i 0]
    (when (< i (count table))
      (loop [j 0]
        (when (< j (count (nth table i)))
          (print (format "|%-40s| " (first (vals (nth (nth table i) j)))))
          (recur (+ j 1))))
      (println "")
      (recur (+ i 1)))))



(defn twoDFintoOne [df1, df2, clause]
  (cond
    (= clause "and") (filterv (fn [x] (.contains df2 x)) df1)
    (= clause "or") (into [] (concat df1 df2))))

(defn recursiveDFs [dfs, clauses, currentDF]
  (if (= (count clauses) 0)
    currentDF
    (recursiveDFs (subvec dfs 0), (subvec clauses 0),(twoDFintoOne currentDF, (first dfs), (first clauses)))))

(defn recursiveWheresParser [wheres, clauseslist, andorlist, currentClause]
  (if (= (count wheres) 0)
    (if (= (count currentClause) 0)
      [(into [] (reverse currentClause)), (into [] (reverse andorlist))]
      [(into [] (reverse (conj clauseslist (into [] (reverse currentClause))))), (into [] (reverse andorlist))])

    (let [currentToken (last wheres)]
      (case currentToken
        "and" (recursiveWheresParser (pop wheres)
                                     (conj clauseslist (into [] (reverse currentClause)))
                                     (conj andorlist "and")
                                     [])
        "or" (recursiveWheresParser (pop wheres)
                                    (conj clauseslist (into [] (reverse currentClause)))
                                    (conj andorlist "or")
                                    [])
        (recursiveWheresParser (pop wheres)
                               clauseslist
                               andorlist
                               (conj currentClause (last wheres)))))))

(defn oneExpressionParser [table, expression]
  (if (= (nth expression 0) "not")
    (do
      (def ColumnName (keyword (nth expression 1)))
      (def Sign (nth expression 2))
      (def Value  (nth expression 3))
      (cond
        (= Sign "=") (filterv (fn [row] (not= (get row ColumnName) Value)) table)
        (= Sign ">") (filterv (fn [row] (<= (Integer/parseInt (get row ColumnName)) (Integer/parseInt Value))) table)))
    (do
      (def ColumnName (keyword (nth expression 0)))
      (def Sign (nth expression 1))
      (def Value  (nth expression 2))
      (cond
        (= Sign "=") (filterv (fn [row] (= (get row ColumnName) Value)) table)
        (= Sign ">") (filterv (fn [row] (> (Integer/parseInt (get row ColumnName)) (Integer/parseInt Value))) table)))))

(defn whereExpressionParser [table, query, sqlParams]
  (def whereIndex (.indexOf query "where"))
  (def whereClause (subvec query (+ 1 whereIndex)))
  (def normalWhere (if (contains? sqlParams :hasOrder)
                     (subvec whereClause 0 (.indexOf query "order"))
                     whereClause))
  (def secondNormal (if (contains? sqlParams :hasGroup)
                      (subvec whereClause 0 (.indexOf query "group"))
                      normalWhere))
  (def expressionsAndConnects (recursiveWheresParser secondNormal, [], [], []))
  (def allDFs (mapv (partial oneExpressionParser table) (first expressionsAndConnects)))
  (def firstwhereDF (if (= (count allDFs) 1)
                      (nth allDFs 0)
                      (twoDFintoOne (nth allDFs 0), (nth allDFs 1), (first (nth expressionsAndConnects 1)))))
  (if (= (count allDFs) 1)
    firstwhereDF
    (recursiveDFs (subvec allDFs 2), (subvec (nth expressionsAndConnects 1) 1), firstwhereDF)))


(defn selectCols [cols, oneline]
  (select-keys oneline cols))

(defn oneRowSelect [colnames, onerow]
  (cond
    (empty? colnames) []
    (= (first colnames) "*") (into []
                                   (oneRowSelect (concat (keys onerow) (rest colnames)) onerow))
    :else (into [] (cons {(keyword (first colnames)) (get onerow (keyword (first colnames)))}
                         (oneRowSelect (rest colnames) onerow)))))

(defn newSimpleExpressions [colnames, table]
  (mapv (partial oneRowSelect colnames) table))

(defn SimpleExpressions [cols, wholeTable]
  (def newcols (mapv keyword cols))
  (mapv (partial selectCols newcols) wholeTable))




(defn distinctSelect [tableName]
  (into [] (distinct tableName)))




(defn simpleColsParser [query]
  (def columns
    (apply list (str/split
                 (str/join ""
                           (subvec query 1

                                   (.indexOf query "from"))) #",")))

  (cond
    (and (str/includes? (str/join "" columns) "(") (str/includes? (str/join "" columns) ")")) nil;;for formulas
    (not= count (str/join "" columns) 0) columns;
    :else (throw (.Trowable "some kind of error about columns"))))



(defn getTableIndex [query]
  (+ (.indexOf query "from") 1));

(defn parseRequest [query]
  (cond
    (some? (some (partial = "select") query)) (if (some? (some (partial = "distinct") query))
                                                (conj {:isSelect true}
                                                      (parseRequest (into []  (subvec query 1))))
                                                (conj {:isSelect true}
                                                      (parseRequest (into [] (concat ["nextexps"] (subvec query 1))))))
    (some? (some (partial = "distinct") query)) (conj {:isDistinct true}
                                                      (parseRequest (into [] (concat ["nextexps"] (subvec query 1)))))
    (some? (some (partial = "nextexps") query)) (conj {:expressions (simpleColsParser query)}
                                                      (parseRequest (subvec query 1)))
    (some? (some (partial = "from") query)) (conj {:tableName (get query (getTableIndex query))}
                                                  (parseRequest (into [] (concat (subvec query 0 (- (getTableIndex query) 1))
                                                                                 (subvec query (getTableIndex query))))))
    (some? (some (partial = "where") query)) (conj {:hasWhere true}
                                                   (parseRequest (into [] (concat (subvec query 0 (.indexOf query "where"))
                                                                                  (subvec query (+ (.indexOf query "where") 1))))))
    ;;                                                                            
    ;; (some? (some (partial = "case") query)) (conj {:hasCase true}
    ;;                                               (parseRequest (into [] (concat (subvec query 0 (.indexOf query "case"))
    ;;                                                                              (subvec query (+ (.indexOf query "case") 1))))))
    (some? (some (partial = "group") query)) (conj {:hasGroup true}
                                                   (parseRequest (into [] (concat (subvec query 0 (.indexOf query "group"))
                                                                                  (subvec query (+ (.indexOf query "group") 1))))))
    (some? (some (partial = "order") query)) (conj {:hasOrder true}
                                                   (parseRequest (into [] (concat (subvec query 0 (.indexOf query "order"))
                                                                                  (subvec query (+ (.indexOf query "order") 1))))))
    ;; (some? (some (partial = "order") query)) ()
    :else {}))


(defn ParseSQL [inputString]
  (def queryArray (str/split inputString #" "))
  (def sql (parseRequest queryArray))
  (def tableDF  (if (and (contains? sql :isSelect)
                         (contains? sql :tableName))
                  (makeTable (get sql :tableName) (str (nth (str/split (get sql :tableName) #"\.") 1)))
                  (throw (Throwable. "No select or table name"))))
  (def whereDF (if (contains? sql :hasWhere)
                 (whereExpressionParser tableDF queryArray sql)
                 tableDF))
  (def distinctDF (if (contains? sql :isDistinct)
                    (distinctSelect whereDF)
                    whereDF))
  (def ExprDF (if (contains? sql :isFormula)
                distinctDF
                (newSimpleExpressions (get sql :expressions) distinctDF)))
  (printTable ExprDF))





(defn -main
  []
  (println "Input your query: ")
  (def input (read-line))
  (ParseSQL input))