(ns piped.utils
  "Utility functions."
  (:require [clojure.core.async :as async])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [java.util UUID]))

(def visibility-timeout-seconds 30)
(def minimum-messages-received 1)
(def maximum-messages-received 10)
(def minimum-wait-time-seconds 0)
(def maximum-wait-time-seconds 20)
(def deadline-safety-buffer 2000)

(defn message->queue-url [message]
  (some-> message meta :queue-url))

(defn message->deadline [message]
  (some-> message meta :deadline))

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

(defn dev-null []
  (async/chan (async/dropping-buffer 0)))

(defn channel? [c]
  (instance? ManyToManyChannel c))

(defn backoff-seq
  "Returns an infinite seq of exponential back-off timeouts with random jitter."
  [max]
  (->>
    (lazy-cat
      (->> (cons 0 (iterate (partial * 2) 1000))
           (take-while #(< % max)))
      (repeat max))
    (map (fn [x] (+ x (rand-int 1000))))))

(defn distinct-by
  "Like distinct but according to a key-fn instead of the element itself."
  ([f]
   (fn [rf]
     (let [seen (volatile! #{})]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result x]
          (let [fx (f x) k (hash fx)]
            (if (contains? @seen k)
              result
              (do (vswap! seen conj k)
                  (rf result x)))))))))
  ([f coll]
   (let [step (fn step [xs seen]
                (lazy-seq
                  ((fn [[x :as xs] seen]
                     (when-let [s (seq xs)]
                       (let [fx (f x) k (hash fx)]
                         (if (contains? seen k)
                           (recur (rest s) seen)
                           (cons x (step (rest s) (conj seen k)))))))
                   xs seen)))]
     (step coll #{}))))

(defn deadline-batching
  "Batches messages from chan and emits the most recently accumulated batch whenever
   the max batch size is reached or one of the messages in the batch has become 'due'
   for action. deadline-fn is a function of a message that returns a channel that
   closes when the message is 'due'. deadline-fn may return nil if a message has no
   particular urgency."
  [chan max deadline-fn]
  (let [return (async/chan)]
    (async/go-loop [channels [chan] batch []]
      (if (= max (count batch))
        (when (async/>! return batch)
          (recur [chan] []))
        (if-some [[value port] (async/alts! channels :priority true)]
          (if-not (identical? port chan)
            (if (not-empty batch)
              (when (async/>! return batch)
                (recur [chan] []))
              (recur [chan] []))
            (if (some? value)
              (if-some [deadline (deadline-fn value)]
                (recur (conj channels deadline) (conj batch value))
                (recur channels (conj batch value)))
              (do (when (not-empty batch)
                    (async/>! return batch))
                  (async/close! return)))))))
    return))

(defn interval-batching
  "Partitions the original chan by non-empty time intervals."
  ([chan msecs]
   (interval-batching chan msecs nil))
  ([chan msecs max]
   (let [return (async/chan)]
     (async/go-loop [deadline (async/timeout msecs) batch []]
       (if-some [result (async/alt! [chan] ([v] v) [deadline] ::timeout :priority true)]
         (case result
           ::timeout
           (if (empty? batch)
             (recur (async/timeout msecs) [])
             (when (async/>! return batch)
               (recur (async/timeout msecs) [])))
           (let [new-batch (conj batch result)]
             (if (and max (= max (count new-batch)))
               (when (async/>! return new-batch)
                 (recur (async/timeout msecs) []))
               (recur deadline new-batch))))
         (do (when (not-empty batch)
               (async/>! return batch))
             (async/close! return))))
     return)))

(defmacro on-chan-close
  "Execute body when the channel is closed. This is immediate upon
   channel closing and occurs on the same thread that closed chan."
  [chan & body]
  `(let [id#      (UUID/randomUUID)
         channel# ~chan]
     (add-watch
       (.closed ^ManyToManyChannel channel#) id#
       (^:once fn* [k# r# o# n#]
         (when (and (false? o#) (true? n#))
           (try ~@body (finally (remove-watch r# id#))))))
     channel#))

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