(ns piped.utils
  "Utility functions."
  (:require [clojure.core.async :as async])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [java.util UUID]))

(defn message->queue-url [message]
  (some-> message meta :queue-url))

(defn message->deadline [message]
  (some-> message meta :deadline))

(defn message->identifier [message]
  (or (some-> message :MessageId) (str (UUID/randomUUID))))

(defn message->timeout [message]
  (some-> message meta :timeout))

(defn with-deadline [message duration]
  (vary-meta message assoc :deadline (async/timeout duration)))

(defn with-timeout [message timeout]
  (vary-meta message assoc :timeout timeout))

(defn anomaly? [response]
  (contains? response :cognitect.anomalies/category))

(defn clamp [start end x]
  (min (max start x) end))

(defn average [& args]
  (let [agg (reduce (fn [agg x]
                      (-> agg
                          (update :sum + x)
                          (update :count inc)))
                    {:sum 0 :count 0}
                    args)]
    (/ (:sum agg) (:count agg))))

(defn average+ [& args]
  (long (Math/ceil (apply average args))))

(defn average- [& args]
  (long (Math/floor (apply average args))))

(defn quot+ [& nums]
  (long (Math/ceil (apply / nums))))

(defn channel? [c]
  (instance? ManyToManyChannel c))

(defn backoff-seq
  "Returns an infinite seq of exponential back-off timeouts with random jitter."
  ([] (backoff-seq 60000))
  ([max]
   (->>
    (lazy-cat
     (->> (cons 0 (iterate (partial * 2) 1000))
          (take-while #(< % max)))
     (repeat max))
    (map (fn [x] (+ x (rand-int 1000)))))))

(defn deadline-batching
  "Batches messages from chan and emits the most recently accumulated batch whenever
   the max batch size is reached or one of the messages in the batch has become 'due'
   for action."
  [chan max]
  (let [return (async/chan)]
    (async/go-loop [channels [chan] batch {}]
      (if (= max (count batch))
        (when (async/>! return (vals batch))
          (recur [chan] {}))
        (if-some [[value port] (async/alts! channels :priority true)]
          ;; Drew from a deadline.
          (if-not (identical? port chan)
            (if (seq batch)
              (when (async/>! return (vals batch))
                (recur [chan] {}))
              (recur [chan] {}))
            (if (some? value)
              (let [identifier (message->identifier value)
                    new-batch  (assoc batch identifier value)]
                (if-some [deadline (message->deadline value)]
                  (recur (conj channels deadline) new-batch)
                  (recur channels new-batch)))
              (do (when (seq batch)
                    (async/>! return (vals batch)))
                  (async/close! return)))))))
    return))

(defn interval-batching
  "Partitions the original chan by non-empty time intervals."
  ([chan msecs]
   (interval-batching chan msecs nil))
  ([chan msecs max]
   (let [return (async/chan)]
     (async/go-loop [deadline (async/timeout msecs)
                     batch {}]
       (if-some [result (async/alt! [chan] ([v] v)
                                    [deadline] ::timeout
                                    :priority true)]
         (case result
           ::timeout
           (if (empty? batch)
             (recur (async/timeout msecs) {})
             (when (async/>! return (vals batch))
               (recur (async/timeout msecs) {})))
           (let [new-batch (assoc batch (message->identifier result) result)]
             (if (and max (= max (count new-batch)))
               (when (async/>! return (vals new-batch))
                 (recur (async/timeout msecs) {}))
               ;; Continue accumulating with deadline.
               (recur deadline new-batch))))
         (do (when (not-empty batch)
               (async/>! return (vals batch)))
             (async/close! return))))
     return)))

(defmacro defmulti*
  "Like clojure.core/defmulti, but actually updates the dispatch value when you reload it."
  [symbol dispatch-fn]
  `(let [dispatch-fun# ~dispatch-fn
         existing-var# (resolve '~symbol)]
     (if-some [dispatch# (some-> existing-var# meta ::holder)]
       (do (vreset! dispatch# dispatch-fun#) existing-var#)
       (let [holder# (volatile! dispatch-fun#)
             var#    (defmulti ~symbol (fn [& args#] (apply @holder# args#)))]
         (alter-meta! var# merge {::holder holder#})
         var#))))
