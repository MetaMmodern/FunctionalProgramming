  
(ns lab3.core
  (:gen-class)
  (:use [clojure.data.csv])
  (:require [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.set :as set]))


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

;; Convert Ints
(defn OneCellToInt [str]
  (if (empty? str) false (every? #(Character/isDigit %) str)))

(defn OneRowToInts [row]
  (def thisVals (vals row))
  (def thisKeys (keys row))
  (zipmap thisKeys (mapv #(if (OneCellToInt %)
                            (Integer/parseInt %)
                            %) thisVals)))

(defn TabletoInt [table]
  (mapv OneRowToInts table))

;; Convert EmptyValues
(defn oneRowNonEmpty [row]
  (into {} (filter #(not= (second %) "")
                   row)))
(defn wholeTableNonEmpty [table]
  (into [] (map oneRowNonEmpty table)))

;; JOINS


(defn innerJoin [ds1 ds2 col1 col2]
  (into [] (clojure.set/join ds1 ds2 {col1 col2})))

(defn outterJoin [ds1 ds2 col1 col2]
  (let [z1 (zipmap (map col1 ds1) ds1)
        z2 (zipmap (map col2 ds2) ds2)]
    (vals (merge-with merge z1 z2))))


(defn rightJoin [ds1 ds2 col1 col2]
  (def out (outterJoin ds1 ds2 col1 col2))
  (into [] (filterv #(some (partial = (first (vals %))) (map col2 ds2)) out)))

(defn allKeysChecker [allkeys, row]
  (if (empty? allkeys)
    (into (sorted-map) row)
    (if (contains? row (first allkeys))
      (allKeysChecker (rest allkeys) row)
      (allKeysChecker (rest allkeys) (conj {(first allkeys) ""} row)))))

(defn getJoiningRightTable [query]
  (get query (+ (.indexOf query "join") 1)))

(defn ColumnsForJoin [query]
  (def left (nth (str/split (get query (+ (.indexOf query "on") 1)) #"\.") 1))
  (def right (nth (str/split (get query (+ (.indexOf query "on") 3)) #"\.") 1))
  {:left (keyword left) :right (keyword right)})

(defn getJoinedTable [TableOne, sql, query]
  (def joinType (get query (- (.indexOf query "join") 1)))
  (def clearTableOne (wholeTableNonEmpty TableOne))
  (def TableTwo  (TabletoInt (makeTable (get sql  :joinedTable) (str (nth (str/split (get sql :joinedTable) #"\.") 1)))))
  (def allkeys (distinct (concat (keys (first TableOne)) (keys (first TableTwo)))))
  (def clearTableTwo (wholeTableNonEmpty TableTwo))
  (case joinType
    "outter" (into [] (map (partial allKeysChecker allkeys) (outterJoin clearTableOne clearTableTwo (get (get sql :valsJoin) :left) (get (get sql :valsJoin) :right))))

    "inner" (into [] (map (partial allKeysChecker allkeys) (innerJoin clearTableOne clearTableTwo (get (get sql :valsJoin) :left) (get (get sql :valsJoin) :right))))
    "right" (into [] (map (partial allKeysChecker allkeys) (rightJoin clearTableOne clearTableTwo (get (get sql :valsJoin) :left) (get (get sql :valsJoin) :right))))))

;; FORMULAS
(defn average
  [numbers]
  (if (empty? numbers)
    0
    (double (/ (reduce + numbers) (count numbers)))))

(defn HigherOrderFun [fun, col, table]
  (def allNums (mapv (fn [row] (get row (keyword col))) table))
  [[{(keyword col) (fun allNums)}]])


;; ORDER PARSER
(defn OrderMaker [table, cols, direction]
  (def colnames (str/split (str/join "" cols) #","))
  (def colkeys   (mapv keyword colnames))

  (if (= direction "asc")
    (sort-by (apply juxt colkeys) table)
    (into [] (reverse (sort-by (apply juxt colkeys) table)))))

(defn OrderParser [table, query]
  (def OrderIndex  (.indexOf query "order"))
  (def queryWithoutKeywords (subvec query (+ 2 OrderIndex)))
  (def prevLast (last queryWithoutKeywords))
  (if (or (= prevLast "asc") (= prevLast "desc"))
    (OrderMaker table, (into [] (drop-last queryWithoutKeywords)), prevLast)
    (OrderMaker table, queryWithoutKeywords, "asc")))

;; WHERE PARSER
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
        (= Sign "=") (filterv (fn [row] (not= (get row ColumnName) (Integer/parseInt Value))) table)
        (= Sign ">") (filterv (fn [row] (<= (get row ColumnName) (Integer/parseInt Value))) table)))
    (do
      (def ColumnName (keyword (nth expression 0)))
      (def Sign (nth expression 1))
      (def Value  (nth expression 2))
      (cond
        (= Sign "=") (filterv (fn [row] (= (get row ColumnName) (Integer/parseInt Value)))  table)
        (= Sign ">") (filterv (fn [row] (> (get row ColumnName) (Integer/parseInt Value))) table)))))

(defn whereExpressionParser [table, query, sqlParams]
  (def whereIndex (.indexOf query "where"))
  (def whereClause (subvec query (+ 1 whereIndex)))
  (def normalWhere (if (contains? sqlParams :hasOrder)
                     (subvec whereClause 0 (.indexOf whereClause "order"))
                     whereClause))
  (def secondNormal (if (contains? sqlParams :hasGroup)
                      (subvec whereClause 0 (.indexOf normalWhere "group"))
                      normalWhere))
  (def expressionsAndConnects (recursiveWheresParser secondNormal, [], [], []))
  (def allDFs (mapv (partial oneExpressionParser table) (first expressionsAndConnects)))
  (def firstwhereDF (if (= (count allDFs) 1)
                      (nth allDFs 0)
                      (twoDFintoOne (nth allDFs 0), (nth allDFs 1), (first (nth expressionsAndConnects 1)))))
  (if (= (count allDFs) 1)
    firstwhereDF
    (recursiveDFs (subvec allDFs 2), (subvec (nth expressionsAndConnects 1) 1), firstwhereDF)))



(defn oneRowSelect [colnames, onerow]
  (cond
    (empty? colnames) []
    (= (first colnames) "*") (into []
                                   (oneRowSelect (concat (keys onerow) (rest colnames)) onerow))
    :else (into [] (cons {(keyword (first colnames)) (get onerow (keyword (first colnames)))}
                         (oneRowSelect (rest colnames) onerow)))))

(defn newSimpleExpressions [colnames, table]
  (mapv (partial oneRowSelect colnames) table))





(defn distinctSelect [tableName]
  (into [] (distinct tableName)))


(defn FormulaParser [expressions, table]
  (def colAndFormula (first expressions))
  (def colname (last (re-find #"\(([^)]+)\)" colAndFormula)))
  (def formula (re-find #"^[^\(]+" colAndFormula))
  (case formula
    "count" (HigherOrderFun count, colname,  table)
    "avg" (HigherOrderFun average, colname, table)
    "min" (HigherOrderFun (fn [x] (apply min x)), colname, table)))


(defn simpleColsParser [query]
  (def columns
    (apply list (str/split
                 (str/join ""
                           (subvec query 1

                                   (.indexOf query "from"))) #",")))
  (if (not= count (str/join "" columns) 0)
    columns
    (throw (.Trowable "some kind of error about columns"))))



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
                                                  (parseRequest (into [] (concat (subvec query 0 (- (getTableIndex query) 2))
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
    (some? (some (partial = "avg") query)) (conj {:hasFormula "avg"}
                                                 (parseRequest (into [] (concat (subvec query 0 (.indexOf query "avg"))
                                                                                (subvec query (+ (.indexOf query "avg") 1))))))
    (some? (some (partial = "min") query)) (conj {:hasFormula "min"}
                                                 (parseRequest (into [] (concat (subvec query 0 (.indexOf query "min"))
                                                                                (subvec query (+ (.indexOf query "min") 1))))))
    (some? (some (partial = "count") query)) (conj {:hasFormula "count"}
                                                   (parseRequest (into [] (concat (subvec query 0 (.indexOf query "count"))
                                                                                  (subvec query (+ (.indexOf query "count") 1))))))
    (some? (some (partial = "join") query)) (conj {:hasJoin true :joinedTable (getJoiningRightTable query) :valsJoin (ColumnsForJoin  query)}
                                                  (parseRequest (into [] (concat (subvec query 0 (+ (getTableIndex query) 1))
                                                                                 (subvec query (+ (getTableIndex query) 3))))))
    ;; (some? (some (partial = "order") query)) ()
    :else {}))


(defn ParseSQL [inputString]
  (def queryArray (str/split inputString #" "))
  (def sql (parseRequest queryArray))
  (def tableDF  (if (and (contains? sql :isSelect)
                         (contains? sql :tableName))
                  (makeTable (get sql :tableName) (str (nth (str/split (get sql :tableName) #"\.") 1)))
                  (throw (Throwable. "No select or table name"))))
  (def IntStrDF (TabletoInt tableDF))
  (def JoinsDF (if (contains? sql :hasJoin)
                 (getJoinedTable IntStrDF sql queryArray)
                 IntStrDF))
  (def whereDF (if (contains? sql :hasWhere)
                 (whereExpressionParser JoinsDF queryArray sql)
                 JoinsDF))

  (def OrderedDF (if (contains? sql :hasOrder)
                   (OrderParser whereDF, queryArray)
                   whereDF))

  (def ExprDF (if (contains? sql :hasFormula)
                (FormulaParser (into [] (get sql :expressions)) OrderedDF)
                (newSimpleExpressions (get sql :expressions) OrderedDF)))
  (def distinctDF (if (contains? sql :isDistinct)
                    (distinctSelect ExprDF)
                    ExprDF))
  (printTable distinctDF))





(defn -main
  []
  (println "Input your query: ")
  (def input (read-line))
  (ParseSQL input))