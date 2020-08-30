(ns piped.consumers
  "Code relating to reading SQS messages from channels and processing them."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]
            [piped.sqs :as sqs]))

(defn- spawn-consumer* [client input-chan message-fn]
  (utils/thread-loop [msg (async/<!! input-chan)]
    (let [deadline-chan (-> msg meta :deadline)
          task-chan     (message-fn msg)
          action        (async/alt!!
                          [task-chan] ([v] v)
                          [deadline-chan] :extend
                          :priority true)]
      (case action
        :ack
        (do (sqs/ack-one client msg)
            (recur (async/<!! input-chan)))
        :nack
        (do (sqs/nack-one client msg)
            (recur (async/<!! input-chan)))
        :extend
        (do (sqs/change-visibility-one client msg 30)
            (recur msg))))))

(defn- ->processor
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

