(ns piped.sqs
  "Functions relating to interacting with SQS."
  (:require [clojure.core.async :as async]
            [cognitect.aws.client.api :as aws]))


(defn deadline [message percentage]
  (async/timeout 30000))

(defn nack-one [client queue-url {:keys [ReceiptHandle] :as message}]
  (let [request {:op      :ChangeMessageVisibilityBatch
                 :request {:QueueUrl          queue-url
                           :ReceiptHandle     ReceiptHandle
                           :VisibilityTimeout 0}}]
    (aws/invoke client request)))

(defn nack-many [client queue-url messages]
  )

