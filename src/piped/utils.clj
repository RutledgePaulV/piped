(ns piped.utils
  "Utility functions."
  (:require [clojure.core.async :as async]))

(defn message->queue-url [message]
  (some-> message meta :queue-url))

(defn bounded-inc [x bound]
  (min (inc x) bound))

(defn bounded-dec [x bound]
  (max (dec x) bound))

(defmacro thread-loop [bindings & body]
  `(async/thread (loop ~bindings ~@body)))

(defn drain-buffer
  "Reads all the messages currently in the buffer. Does not block."
  [chan]
  (loop [messages []]
    (if-some [msg (async/poll! chan)]
      (recur (conj messages msg))
      messages)))

(defn deadline-batching
  "Batches messages from chan and emits the most recently accumulated batch whenever
   the max batch size is reached or one of the messages in the batch has become 'due'
   for action."
  [chan max key-fn]
  (let [return (async/chan)]
    (async/go-loop [mix (async/mix (async/chan)) batch []]
      (let [timeout (async/muxch* mix)]
        (if (= max (count batch))
          (do (async/>! return batch)
              (recur (async/mix timeout) []))
          (if-some [result (async/alt! chan ([v] v) timeout ([_] ::timeout) :priority true)]
            (if (= result ::timeout)
              (do (when (not-empty batch) (async/>! return batch))
                  (recur (async/mix (async/muxch* mix)) []))
              (let [deadline (key-fn result)]
                (recur (doto mix (async/admix deadline)) (conj batch result))))
            (when (not-empty batch)
              (async/>! return batch))))))
    return))

(defn batching
  "Partitions the original chan by non-empty time intervals."
  ([chan msecs]
   (batching chan msecs nil))
  ([chan msecs max]
   (let [return (async/chan)]
     (async/go-loop [deadline (async/timeout msecs) batch []]
       (if-some [result (async/alt! [chan] ([v] v) [deadline] ::timeout :priority true)]
         (cond
           (and (#{::timeout} result) (empty? batch))
           (recur (async/timeout msecs) [])
           (and (#{::timeout} result) (not-empty batch))
           (do (async/>! return batch) (recur (async/timeout msecs) []))
           :otherwise
           (let [new-batch (conj batch result)]
             (if (and max (= max (count new-batch)))
               (do (async/>! return new-batch)
                   (recur (async/timeout msecs) []))
               (recur deadline new-batch))))
         (when (not-empty batch)
           (async/>! return batch))))
     return)))


