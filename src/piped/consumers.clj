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
          (do (async/<! (sqs/change-visibility-one client msg 30))
              (recur (utils/with-deadline msg (- (* 30 1000) 400)) task)))))))

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
  "Spawns a consumer fit for synchronous cpu bound tasks. Uses a core.async dispatch thread when processing a message.

    :client        - an aws-api sqs client instance
    :producer-chan - a channel of incoming sqs messages
    :processor     - a function of a message that runs pure computation on the message

  "
  [client input-chan ack-chan nack-chan processor]
  (make-consumer client input-chan ack-chan nack-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/go (lifted msg))))))


(defn spawn-consumer-blocking
  "Spawns a consumer fit for synchronous blocking tasks. Uses a dedicated thread when processing a message.

    :client        - an aws-api sqs client instance
    :producer-chan - a channel of incoming sqs messages
    :processor     - a function of a message that may perform blocking side effects with the message

  "
  [client input-chan ack-chan nack-chan processor]
  (make-consumer client input-chan ack-chan nack-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/thread (lifted msg))))))

