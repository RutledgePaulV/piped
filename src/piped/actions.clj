(ns piped.actions
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [piped.utils :as utils]
            [piped.sqs :as sqs]))

(defn spawn-acker
  "Acks messages in batches. As soon as one of the messages in a batch becomes 'due'
   then the entire batch is consumed and acked. If 10 messages accumulate before one
   becomes due then the batch is acked anyways since only 10 at a time can be acked."
  [client input-chan]
  (async/go-loop []
    (when-some [batch (async/<! input-chan)]
      (let [response (async/<! (sqs/ack-many client batch))]
        (when (utils/anomaly? response)
          (log/error "Error when trying to ack batch of messages." (pr-str response))))
      (recur))))

(defn spawn-nacker
  "Nacks messages in batches. By default will accumulate messages to nack every 5
   seconds or every 10 messages. Nacking is typically not a high priority operation
   so this is mainly about not making a lot of network calls."
  [client input-chan]
  (async/go-loop []
    (when-some [batch (async/<! input-chan)]
      (let [response (async/<! (sqs/nack-many client batch))]
        (when (utils/anomaly? response)
          (log/error "Error when trying to nack batch of messages." (pr-str response))))
      (recur))))

