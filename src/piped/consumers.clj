(ns piped.consumers
  "Code relating to reading SQS messages from channels and processing them."
  (:require [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]
            [piped.sqs :as sqs]))

(defn- get-deadline [message]
  (some-> message meta :deadline))

(defn- make-consumer
  [client input-chan message-fn]
  (let [acks       (async/chan 1)
        ack-batch  (utils/deadline-batching acks 10 get-deadline)
        nacks      (async/chan 1)
        nack-batch (utils/batching nacks 5000 10)]

    (async/go-loop [msg (async/<! input-chan) task (message-fn msg)]

      (let [deadline-chan (get-deadline msg)
            [action data] (async/alt!
                            [ack-batch] ([batch] [:ack-batch batch])
                            [task] ([action] [action])
                            [deadline-chan] [:extend]
                            [nack-batch] ([batch] [:nack-batch batch])
                            :priority true)]

        (case action
          :ack-batch
          ; perform the acknowledgement since at least one has become due or our batch is full
          (do (async/<! (sqs/ack-many client data))
              (recur msg task))

          ; perform the nack since we've accumulated enough or waited long enough to accumulate some
          :nack-batch
          (do (async/<! (sqs/nack-many client data))
              (recur msg task))

          :ack
          ; enqueue an acknowledgement for this message
          (do (async/>! acks msg)
              (when-some [new-message (async/<! input-chan)]
                (recur new-message (message-fn msg))))

          :nack
          ; nack it so it becomes available for other consumers or will DLQ if out of retries
          (do (async/>! nacks msg)
              (when-some [new-message (async/<! input-chan)]
                (recur new-message (message-fn msg))))

          :extend
          ; need to extend visibility of this message since it's still in-flight
          (do (async/<! (sqs/change-visibility-one client msg 30))
              (let [new-deadline (async/timeout (- (* 30 1000) 400))]
                (recur (vary-meta msg assoc :deadline new-deadline) task))))))))

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
  "Spawns a consumer fit for asynchronous tasks. processor must be instantaneous and should not perform work prior to
   returning with a channel, this is for advanced users only who already have a non-blocking stack. Most users should
   use spawn-consumer-compute and spawn-consumer-blocking.

    :client        - an aws-api sqs client instance
    :producer-chan - a channel of incoming sqs messages
    :processor     - a function of a message that returns a core.async channel that emits once (like a promise chan)

  "
  [client producer-chan processor]
  (make-consumer client producer-chan
    (fn [msg] (async/map (->processor identity) (processor msg)))))


(defn spawn-consumer-compute
  "Spawns a consumer fit for synchronous cpu bound tasks. Uses a core.async dispatch thread when processing a message.

    :client        - an aws-api sqs client instance
    :producer-chan - a channel of incoming sqs messages
    :processor     - a function of a message that runs pure computation on the message

  "
  [client producer-chan processor]
  (make-consumer client producer-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/go (lifted msg))))))


(defn spawn-consumer-blocking
  "Spawns a consumer fit for synchronous blocking tasks. Uses a dedicated thread when processing a message.

    :client        - an aws-api sqs client instance
    :producer-chan - a channel of incoming sqs messages
    :processor     - a function of a message that may perform blocking side effects with the message

  "
  [client producer-chan processor]
  (make-consumer client producer-chan
    (let [lifted (->processor processor)]
      (fn [msg] (async/thread (lifted msg))))))

