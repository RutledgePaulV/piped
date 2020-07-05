(ns piped.consumers
  "Code relating to reading SQS messages from channels and processing them."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]))

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

(defn ->processor
  "Turns a function with unknown behavior into a predictable
   function with well-defined return values and no exceptions."
  [processor-fn]
  (fn [msg]
    (try
      (let [result (processor-fn msg)]
        (if (contains? #{:ack :nack} result)
          result
          :ack))
      (catch Exception e
        (log/error e "Exception processing sqs message.")
        :nack))))

(defn spawn-consumer-compute
  "Spawns a consumer fit for cpu bound tasks."
  [client producer-chan processor]
  (spawn-consumer* client producer-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/go (lifted msg))))))

(defn spawn-consumer-blocking
  "Spawns a consumer fit for blocking io tasks."
  [client producer-chan processor]
  (spawn-consumer* client producer-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/thread (lifted msg))))))

