(ns piped.buffers
  (:require [clojure.core.async.impl.protocols :as impl])
  (:import (java.util PriorityQueue Comparator)
           (clojure.lang Counted)))


(deftype PriorityBuffer [^PriorityQueue buf ^long n]
  impl/Buffer
  (full? [this]
    (>= (.size buf) n))
  (remove! [this]
    (.poll buf))
  (add!* [this itm]
    (.offer buf itm)
    this)
  (close-buf! [this])
  Counted
  (count [this]
    (.size buf)))

(defn priority-buffer
  ([n] (priority-buffer n compare))
  ([^long n ^Comparator comparator]
   (let [queue (PriorityQueue. (int n) comparator)]
     (->PriorityBuffer queue n))))

(defn priority-buffer-by
  ([n key-fn]
   (priority-buffer-by n key-fn compare))
  ([n key-fn comparator]
   (priority-buffer n #(comparator (key-fn %1) (key-fn %2)))))


