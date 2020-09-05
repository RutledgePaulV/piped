(ns piped.utils
  "Utility functions."
  (:require [clojure.core.async :as async]))

(defn message->queue-url [message]
  (some-> message meta :queue-url))

(defn message->deadline [message]
  (some-> message meta :deadline))

(defn with-deadline [message duration]
  (vary-meta message assoc :deadline (async/timeout duration)))

(defn clamp [start end x]
  (min (max start x) end))

(defn deadline-batching
  "Batches messages from chan and emits the most recently accumulated batch whenever
   the max batch size is reached or one of the messages in the batch has become 'due'
   for action. key-fn is a function of a message that returns a channel that closes when
   the message is 'due'"
  [chan max key-fn]
  (let [return (async/chan)]
    (async/go-loop [mix (async/mix (async/chan)) batch []]
      (let [timeout (async/muxch* mix)]
        (if (= max (count batch))
          (do
            #_(println "reached max batch size, emitting batch and starting new round")
            (when (async/>! return batch)
              (recur (async/mix (async/chan)) [])))
          (do
            #_(println "waiting for message or batch timeout, whichever comes first.")
            (if-some [result (async/alt! chan ([v] v) timeout ([_] ::timeout) :priority true)]
              (if (= result ::timeout)
                (do
                  #_(println "batch accumulation timed out, emitting batch and starting new round")
                  (if (not-empty batch)
                    (when (async/>! return batch)
                      (recur (async/mix (async/chan)) []))
                    (recur (async/mix (async/chan)) [])))
                (let [deadline (key-fn result)]
                  #_(println "Received new message, adding to timeout mix and recurring.")
                  (recur (doto mix (async/admix deadline)) (conj batch result))))
              (do
                #_(println "input channel was closed, emitting last batch (if any) and closing return chan.")
                (when (not-empty batch)
                  (async/>! return batch))
                (async/close! return)))))))
    return))

(defn batching
  "Partitions the original chan by non-empty time intervals."
  ([chan msecs]
   (batching chan msecs nil))
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


