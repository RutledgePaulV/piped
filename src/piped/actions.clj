(ns piped.actions
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [piped.utils :as utils]
            [piped.sqs :as sqs]))

(defn spawn-acker
  "Acks batches of messages."
  [client input-chan]
  (async/go-loop []
    (when-some [batch (async/<! input-chan)]
      (let [response (async/<! (sqs/ack-many client batch))]
        (when (utils/anomaly? response)
          (log/error "Error when trying to ack batch of messages." (pr-str response))))
      (recur))))

(defn spawn-nacker
  "Nacks batches of messages."
  [client input-chan backoff-fn]
  (async/go-loop []
    (when-some [batch (async/<! input-chan)]
      (let [response (async/<! (sqs/nack-many client batch backoff-fn))]
        (when (utils/anomaly? response)
          (log/error "Error when trying to nack batch of messages." (pr-str response))))
      (recur))))

