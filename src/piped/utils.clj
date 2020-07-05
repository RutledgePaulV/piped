(ns piped.utils
  "Utility functions.")

(defn message->queue-url [message]
  (some-> message meta :queue-url))

(defn bounded-inc [x bound]
  (min (inc x) bound))

(defn bounded-dec [x bound]
  (max (dec x) bound))


