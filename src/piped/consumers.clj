(ns piped.consumers
  "Code relating to reading SQS messages from channels and processing them."
  (:require [clojure.core.async :as async]))

(defn spawn-consumer* [client producer-chan message-fn]
  (async/go-loop [msg (async/<! producer-chan)]
    (when (some? msg)
      (let [deadline-chan (-> msg meta :deadline)
            task-chan     (message-fn msg)
            action        (async/alt!
                            [task-chan] ([v] v)
                            [deadline-chan] :extend
                            :priority true)]
        (case action
          :ack ()
          :nack ()
          :extend ())))))


(defn spawn-consumer [client producer-chan processor]
  (spawn-consumer* client producer-chan
    (fn [msg]
      (async/go
        (try
          (let [result (processor msg)]
            (if (contains? #{:ack :nack} result)
              result
              :ack))
          (catch Exception e
            :nack))))))

(defn spawn-consumer-blocking [client producer-chan processor]
  (spawn-consumer* client producer-chan
    (fn [msg]
      (async/thread
        (try
          (let [result (processor msg)]
            (if (contains? #{:ack :nack} result)
              result
              :ack))
          (catch Exception e
            :nack))))))

