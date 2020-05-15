(defn map-vals
  [f coll]
  (reduce-kv (fn [acc k v] (assoc acc k (f v))) {} coll))
;;;Table Utilities
;;;A table is defined to be a sequence of maps.  These functions perform
;;;Common operations on tables.
(defn inner-style
  [left-keys right-keys]
  (clojure.set/intersection (set left-keys) (set right-keys)))


(defn right-outer-style
  [left-keys right-keys]
  right-keys)

(defn full-outer-style
  [left-keys right-keys]
  (clojure.set/union (set left-keys) (set right-keys)))

(defn join-worker
  "This is an internal method to be used in each join function."
  ([join-style left-coll right-coll join-fn] (join-worker join-style left-coll right-coll join-fn join-fn))
  ([join-style left-coll right-coll left-join-fn right-join-fn]
   (let [keys-a (keys (first left-coll)) ;The column names of coll-a
         keys-b (keys (first right-coll)) ;The column names of coll-b
         indexed-left (group-by left-join-fn left-coll) ;indexes the coll-a using join-fn-a
         indexed-right (group-by right-join-fn right-coll) ;indexes the coll-b using join-fn-b
         desired-joins (join-style (keys indexed-left) (keys indexed-right))]
     (reduce concat (map (fn [joined-value]
                           (for [left-side (get indexed-left joined-value [{}])
                                 right-side (get indexed-right joined-value [{}])]
                             (merge left-side right-side)))
                         desired-joins)))))

(defmacro defjoin
  [join-name join-style doc-string]
  `(do
     (defn ~join-name
       ~(str doc-string
             "\n  This function takes a left collection, a right collection, and at least one join function.  If only one
join function is provided, it is used on both the left & right hand sides.")
       ([~'left-coll ~'right-coll ~'join-fn]
        (join-worker ~join-style ~'left-coll ~'right-coll ~'join-fn ~'join-fn))
       ([~'left-coll ~'right-coll ~'left-join-fn ~'right-join-fn]
        (join-worker ~join-style ~'left-coll ~'right-coll ~'left-join-fn ~'right-join-fn)))))

(defjoin inner-join inner-style
  "This is for performing inner joins.  The join value must exist in both lists.")
(defjoin right-outer-join right-outer-style
  "This is for performing right outer joins.  The join value must exist in the right hand list.")
(defjoin full-outer-join full-outer-style
  "This is for performing full outer joins.  The join value may exist in either list.")

