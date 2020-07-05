(ns piped.utils
  "Utility functions.")


(defn bounded-inc [x bound]
  (min (inc x) bound))

(defn bounded-dec [x bound]
  (max (dec x) bound))


