  
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
  (let [thisVals (vals row)
        thisKeys (keys row)]
    (zipmap thisKeys (mapv #(if (OneCellToInt %)
                              (Integer/parseInt %)
                              %) thisVals))))

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
  (let [out (outterJoin ds1 ds2 col1 col2)]
    (into [] (filterv #(some (partial = (first (vals %))) (map col2 ds2)) out))))

(defn allKeysChecker [allkeys, row]
  (if (empty? allkeys)
    (into (sorted-map) row)
    (if (contains? row (first allkeys))
      (allKeysChecker (rest allkeys) row)
      (allKeysChecker (rest allkeys) (conj {(first allkeys) ""} row)))))

(defn oneRowJoiner [col1, col2, t2, row]
  (merge row (first (filter (fn [x]
                              (= ((keyword col2) x) ((keyword col1) row))) t2))))

(defn getJoiningRightTable [query]
  (get query (+ (.indexOf query "join") 1)))

(defn ColumnsForJoin [query]
  (let [left (nth (str/split (get query (+ (.indexOf query "on") 1)) #"\.") 1)
        right (nth (str/split (get query (+ (.indexOf query "on") 3)) #"\.") 1)]
    {:left (keyword left) :right (keyword right)}))

(defn getJoinedTable [TableOne, sql, query]
  (let [joinType (get query (- (.indexOf query "join") 1))
        clearTableOne (wholeTableNonEmpty TableOne)
        TableTwo  (TabletoInt (makeTable (get sql  :joinedTable) (str (nth (str/split (get sql :joinedTable) #"\.") 1))))
        allkeys (distinct (concat (keys (first TableOne)) (keys (first TableTwo))))
        clearTableTwo (wholeTableNonEmpty TableTwo)]
    (case joinType
      "outter" (into [] (map (partial allKeysChecker allkeys) (outterJoin clearTableOne clearTableTwo (get (get sql :valsJoin) :left) (get (get sql :valsJoin) :right))))
      "inner" (into [] (map (partial allKeysChecker allkeys) (innerJoin clearTableOne clearTableTwo (get (get sql :valsJoin) :left) (get (get sql :valsJoin) :right))))
      "right" (into [] (map (partial allKeysChecker allkeys) (rightJoin clearTableOne clearTableTwo (get (get sql :valsJoin) :left) (get (get sql :valsJoin) :right)))))))

;; (defn getOneLittleTable [table, colname, colvalue]
;;   (into [] (filter (fn [x] (= (colname x) colvalue)) table)))




;; FORMULAS
(defn average
  [numbers]
  (if (empty? numbers)
    0
    (double (/ (reduce + numbers) (count numbers)))))

(defn HigherOrderFun [fun, col, table]
  (let [allNums (mapv (fn [row] (get row (keyword col))) table)]
    [[{(keyword col) (fun allNums)}]]))


;; ORDER PARSER
(defn OrderMaker [table, cols, direction]
  (let [colnames (str/split (str/join "" cols) #",")
        colkeys   (mapv keyword colnames)]
    (if (= direction "asc")
      (sort-by (apply juxt colkeys) table)
      (into [] (reverse (sort-by (apply juxt colkeys) table))))))

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
  (let [whereIndex (.indexOf query "where")
        whereClause (subvec query (+ 1 whereIndex))
        normalWhere (if (contains? sqlParams :hasOrder)
                      (subvec whereClause 0 (.indexOf whereClause "order"))
                      whereClause)
        secondNormal (if (contains? sqlParams :hasGroup)
                       (subvec whereClause 0 (.indexOf normalWhere "group"))
                       normalWhere)
        expressionsAndConnects (recursiveWheresParser secondNormal, [], [], [])
        allDFs (mapv (partial oneExpressionParser table) (first expressionsAndConnects))
        firstwhereDF (if (= (count allDFs) 1)
                       (nth allDFs 0)
                       (twoDFintoOne (nth allDFs 0), (nth allDFs 1), (first (nth expressionsAndConnects 1))))]
    (if (= (count allDFs) 1)
      firstwhereDF
      (recursiveDFs (subvec allDFs 2), (subvec (nth expressionsAndConnects 1) 1), firstwhereDF))))




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
                                   (if (some? (some (partial = "case") query))
                                     (.indexOf query "case")
                                     (.indexOf query "from")))) #",")))

  (if (not= count (str/join "" columns) 0)
    columns
    (throw (.Trowable "some kind of error about columns"))))



(defn getTableIndex [query]
  (+ (.indexOf query "from") 1));


;; GroupBlock

(defn getGroupByTable [table, sqlParams, queryVector]
  (let [grouper (keyword (:hasGroup sqlParams))
        allCols (simpleColsParser queryVector)
        colname (last (re-find #"\(([^)]+)\)" (first (filter (fn [x] (clojure.string/includes? x "(")) allCols))))
        formula (re-find #"^[^\(]+" (first (filter (fn [x] (clojure.string/includes? x "(")) allCols)))]
    (case formula
      "min" (map
             (fn [[grp-map values]]
               (assoc grp-map
                      (keyword (str formula colname)) (apply min (map (keyword colname) values)))) (group-by #(select-keys % [grouper]) table))
      "count" (map
               (fn [[grp-map values]]
                 (assoc grp-map
                        (keyword (str formula colname)) (count (map (keyword colname) values)))) (group-by #(select-keys % [grouper]) table)))))

(defn HavingDFGetter [table, sqlParams, queryVector]
  (let [havingIndex (.indexOf queryVector "having")
        havingClause (subvec queryVector (+ 1 havingIndex))
        normalHaving (if (contains? sqlParams :hasOrder)
                       (subvec havingClause 0 (.indexOf havingClause "order"))
                       havingClause)

        colName (str (re-find #"^[^\(]+" (nth normalHaving 0)) (last (re-find #"\(([^)]+)\)" (nth normalHaving 0))))
        sign (nth normalHaving 1)
        comparablePart (Integer/parseInt (nth normalHaving 2))]
    (doall (filter #((resolve (symbol sign)) (get % (keyword colName)) comparablePart) table))))

(defn TableCaseNonEmpty [table keyName]
  (mapv #(assoc % keyName "") table))

(defn CheckOneRowOneExpression [row, expr]
  (let [colname (keyword (nth expr 0))
        sign (nth expr 1)
        comparablePart (nth expr 2)
        trueComparablePart (if (every? #(Character/isDigit %) comparablePart)
                             (Integer/parseInt comparablePart)
                             comparablePart)]
    ((resolve (symbol sign)) (colname row) trueComparablePart)))


(defn TableCaseModifier [table keyName AllParams elseCase]
  (if (empty? (nth AllParams 0))
    (into [] (map #(if (= (keyName %) "")
                     (assoc % keyName elseCase)
                     (assoc % keyName (keyName %))) table))
    (TableCaseModifier (map #(if (CheckOneRowOneExpression % (first (nth AllParams 0)))
                               (assoc % keyName (first (nth AllParams 1)))
                               (assoc % keyName (keyName %))) table) keyName [(rest (nth AllParams 0)), (rest (nth AllParams 1))] elseCase)))


(defn WheresConditionsParser [wholeThing, wheres, conditions]
  (if (empty? wholeThing)
    [wheres, conditions]
    (if (= "when" (first wholeThing))
      (WheresConditionsParser (subvec wholeThing 4), (cons (subvec wholeThing 1 4) wheres), conditions) ;;when case
      (if (=  (.indexOf wholeThing "when") -1) ;;then case
        (WheresConditionsParser [], wheres, (cons (clojure.string/join " " (subvec wholeThing 1)) conditions))
        (WheresConditionsParser (subvec wholeThing (.indexOf wholeThing "when")), wheres, (cons (clojure.string/join " " (subvec wholeThing 1 (.indexOf wholeThing "when"))) conditions)))) ;;then case
    ))

(defn CaseParser [table sqlParams queryVector]
  (def WholeCase (subvec queryVector (.indexOf queryVector "case") (.indexOf queryVector "from")))
  (def CaseEnd (subvec WholeCase 0 (.indexOf WholeCase "as")))
  (def AsName (keyword (first (subvec WholeCase (+ 1 (.indexOf WholeCase "as"))))))
  (def WhenWhenElse (subvec CaseEnd
                            (+ 1 (.indexOf CaseEnd "case"))
                            (.indexOf CaseEnd "end")))
  (def ElseClause (clojure.string/join " " (subvec WhenWhenElse (+ 1 (.indexOf WhenWhenElse "else")))))
  (def Whens (subvec WhenWhenElse 1 (.indexOf WhenWhenElse "else")))
  (def allParams (WheresConditionsParser (subvec Whens 3) [(subvec Whens 0 3)] []))
  ;;working with table now
  (def TableWithNewCol (TableCaseNonEmpty table AsName))
  (TableCaseModifier TableWithNewCol AsName allParams ElseClause))



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
    (some? (some (partial = "case") query)) (conj {:hasCase true}
                                                  (parseRequest (into [] (concat (subvec query 0 (.indexOf query "case"))
                                                                                 (subvec query (+ (.indexOf query "case") 1))))))
    (some? (some (partial = "from") query)) (conj {:tableName (get query (getTableIndex query))}
                                                  (parseRequest (into [] (concat (subvec query 0 (- (getTableIndex query) 2))
                                                                                 (subvec query (getTableIndex query))))))
    (some? (some (partial = "where") query)) (conj {:hasWhere true}
                                                   (parseRequest (into [] (concat (subvec query 0 (.indexOf query "where"))
                                                                                  (subvec query (+ (.indexOf query "where") 1))))))
    ;;        
    ;;        
    ;;
    ;;                                                                         
    (some? (some (partial = "group") query)) (conj {:hasGroup (nth query (+ (.indexOf query "group") 2))}
                                                   (parseRequest (into [] (concat (subvec query 0 (.indexOf query "group"))
                                                                                  (subvec query (+ (.indexOf query "group") 1))))))
    (some? (some (partial = "having") query)) (conj {:hasHaving true}
                                                    (parseRequest (into [] (concat (subvec query 0 (.indexOf query "having"))
                                                                                   (subvec query (+ (.indexOf query "having") 1))))))
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

;; '() 
;; []
;; {}

(defn ParseSQL [inputString]
  (let [queryArray (str/split inputString #" ")

        sql (parseRequest queryArray)

        tableDF  (if (and (contains? sql :isSelect)
                          (contains? sql :tableName))
                   (makeTable (get sql :tableName) (str (nth (str/split (get sql :tableName) #"\.") 1)))
                   (throw (Throwable. "No select or table name")))

        IntStrDF (TabletoInt tableDF)

        JoinsDF (if (contains? sql :hasJoin)
                  (getJoinedTable IntStrDF sql queryArray)
                  IntStrDF)

        whereDF (if (contains? sql :hasWhere)
                  (whereExpressionParser JoinsDF queryArray sql)
                  JoinsDF)

        GroupedDF (if (contains? sql :hasGroup)
                    (into [] (getGroupByTable whereDF sql queryArray))
                    whereDF)]
    (if (contains? sql :hasGroup)
      (do
        (def HavingDF (if (contains? sql :hasHaving)
                        (HavingDFGetter GroupedDF sql queryArray)
                        GroupedDF))
        (def OrderedDF (if (contains? sql :hasOrder)
                         (OrderParser HavingDF, queryArray)
                         HavingDF))

        (def ExprDF (newSimpleExpressions (into [] (keys (first OrderedDF))) OrderedDF))
        (def distinctDF (if (contains? sql :isDistinct)
                          (distinctSelect ExprDF)
                          ExprDF))
        (printTable distinctDF))
      (do
        (def OrderedDF (if (contains? sql :hasOrder)
                         (OrderParser GroupedDF, queryArray)
                         GroupedDF))
        (def CaseDF (if (contains? sql :hasCase)
                      (CaseParser OrderedDF sql queryArray)
                      OrderedDF))
        (def ExprDF (if (contains? sql :hasFormula)
                      (FormulaParser (into [] (get sql :expressions)) CaseDF)
                      (newSimpleExpressions (get sql :expressions) CaseDF)))

        (def distinctDF (if (contains? sql :isDistinct)
                          (distinctSelect ExprDF)
                          ExprDF))
        (printTable distinctDF)))))





(defn -main
  []
  (println "Input your query: ")
  (def input (read-line))
  (ParseSQL input))