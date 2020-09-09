(ns piped.consumers
  "Code relating to reading SQS messages from channels and processing them."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]
            [piped.sqs :as sqs]))

(defn- make-consumer
  [client input-chan ack-chan nack-chan message-fn]
  (async/go-loop [msg nil task nil]

    (if (and (nil? msg) (nil? task))
      (if-some [msg (async/<! input-chan)]
        (recur msg (message-fn msg))
        :complete)

      (let [deadline (utils/message->deadline msg)
            action   (async/alt! [task] ([action] action) [deadline] :extend :priority true)]

        (case action
          nil
          (recur nil nil)

          :ack
          (do (async/>! ack-chan msg) (recur nil nil))

          :nack
          (do (async/>! nack-chan msg) (recur nil nil))

          :extend
          ; need to extend visibility of this message because it's still in-flight
          ; we don't want batching for this because immediacy is important here to
          ; avoid the message becoming visible for other consumers
          (let [response (async/<! (sqs/change-visibility-one client msg 30))]
            (when (utils/anomaly? response)
              (log/error "Error extending visibility timeout of inflight message." (pr-str response)))
            (recur (utils/with-deadline msg (- (* 30 1000) 2000)) task)))))))

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


(defn spawn-consumer-async
  "Spawns a consumer fit for cpu bound or asynchronous tasks. Uses the core.async dispatch thread pool.

    :client        - an aws-api sqs client instance
    :input-chan    - a channel of incoming sqs messages
    :ack-chan      - a channel that accepts messages that should be acked
    :nack-chan     - a channel that accepts messages that should be nacked
    :processor     - a function of a message that either returns a result directly
                     or may return a core.async channel that emits once (like a
                     promise chan) when finished processing the message. Must not
                     block.
  "
  [client input-chan ack-chan nack-chan processor]
  (make-consumer client input-chan ack-chan nack-chan
    (let [lifted (->processor identity)]
      (fn [msg]
        (async/map
          lifted
          [(async/go
             (try
               (let [result (processor msg)]
                 (if (utils/channel? result)
                   (async/<! result)
                   result))
               (catch Exception e
                 (log/error e "Exception processing sqs message in async consumer.")
                 :nack)))])))))


(defn spawn-consumer-blocking
  "Spawns a consumer fit for synchronous blocking tasks. Uses a dedicated thread when processing a message.

    :client        - an aws-api sqs client instance
    :input-chan    - a channel of incoming sqs messages
    :ack-chan      - a channel that accepts messages that should be acked
    :nack-chan     - a channel that accepts messages that should be nacked
    :processor     - a function of a message that may perform blocking side effects with the message

  "
  [client input-chan ack-chan nack-chan processor]
  (make-consumer client input-chan ack-chan nack-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/thread (lifted msg))))))

