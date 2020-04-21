(ns lab1.core
  (:gen-class))

(defn list_firsts [one, two, three]
  (map first [one, two, three]))



(def one '(T Y D E F (NL KM LM) JL))
(def two  '(+ 2 3))
(def three  '(* (+ 6 8) (- 70 8)))

(list_firsts one two three)