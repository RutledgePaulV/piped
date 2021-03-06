(ns piped.consumers
  "Code relating to reading SQS messages from channels and processing them."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]
            [piped.sqs :as sqs]))


(defn- make-consumer [client input-chan ack-chan nack-chan message-fn]
  (async/go-loop [msg nil task nil]

    (if (and (nil? msg) (nil? task))
      (if-some [msg (async/<! input-chan)]
        (recur msg (message-fn msg))
        :complete)

      (case (async/alt!
              [task] ([action] action)
              [(utils/message->deadline msg)] :extend
              :priority true)

        nil
        (recur nil nil)

        :ack
        (do (async/>! ack-chan msg) (recur nil nil))

        :nack
        (do (async/>! nack-chan msg) (recur nil nil))

        :extend
        (recur
          (let [message-id  (utils/message->identifier msg)
                queue-url   (utils/message->queue-url msg)
                old-timeout (utils/message->timeout msg)
                new-timeout (* 2 old-timeout)
                response    (async/<! (sqs/change-visibility-one client msg new-timeout))]
            (if (utils/anomaly? response)
              (log/error "Error extending visibility timeout of inflight message." (pr-str response))
              (log/infof "Extended visibility for inflight message %s in queue %s from %d to %d seconds." message-id queue-url old-timeout new-timeout))
            (-> msg (utils/with-deadline (* new-timeout 1000)) (utils/with-timeout new-timeout)))
          task)))))


(defn spawn-consumer-async
  "Spawns a consumer fit for cpu bound or asynchronous tasks. Uses the core.async dispatch thread pool.

    :client        - an aws-api sqs client instance
    :input-chan    - a channel of incoming sqs messages
    :ack-chan      - a channel that accepts messages that should be acked
    :nack-chan     - a channel that accepts messages that should be nacked
    :consumer-fn   - a function of a message that either returns a result directly
                     or may return a core.async channel that emits once (like a
                     promise chan) when finished processing the message. Must not
                     block.
  "
  [client input-chan ack-chan nack-chan consumer-fn]
  (make-consumer client input-chan ack-chan nack-chan
    (fn [msg]
      (async/go
        (try
          (loop [result (consumer-fn msg)]
            (if (utils/channel? result)
              (recur (async/<! result))
              (if (contains? #{:ack :nack} result) result :ack)))
          (catch Exception e
            (log/error e "Exception processing sqs message in async consumer.")
            :nack))))))


(defn spawn-consumer-blocking
  "Spawns a consumer fit for synchronous blocking tasks. Uses a dedicated thread when processing a message.

    :client        - an aws-api sqs client instance
    :input-chan    - a channel of incoming sqs messages
    :ack-chan      - a channel that accepts messages that should be acked
    :nack-chan     - a channel that accepts messages that should be nacked
    :consumer-fn   - a function of a message that may perform blocking io with
                     the message.
  "
  [client input-chan ack-chan nack-chan consumer-fn]
  (make-consumer client input-chan ack-chan nack-chan
    (fn [msg]
      (async/thread
        (try
          (loop [result (consumer-fn msg)]
            (if (utils/channel? result)
              (recur (async/<!! result))
              (if (contains? #{:ack :nack} result) result :ack)))
          (catch Exception e
            (log/error e "Exception processing sqs message in blocking consumer.")
            :nack))))))

