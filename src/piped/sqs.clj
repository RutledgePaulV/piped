(ns piped.sqs
  "Functions relating to interacting with SQS."
  (:require [cognitect.aws.client.api :as aws]))

(defn message->queue-url [message]
  (some-> message meta :queue-url))

(defn change-visibility-one [client {:keys [ReceiptHandle] :as message} visibility-timeout]
  (let [queue-url (some-> message meta :queue-url)
        request   {:op      :ChangeMessageVisibility
                   :request {:QueueUrl          queue-url
                             :ReceiptHandle     ReceiptHandle
                             :VisibilityTimeout visibility-timeout}}]
    (aws/invoke client request)))

(defn change-visibility-batch [client messages visibility-timeout]
  (let [results (->> (for [[queue-url messages] (group-by message->queue-url messages)]
                       (let [request {:op      :ChangeMessageVisibilityBatch
                                      :request {:QueueUrl queue-url
                                                :Entries  (for [{:keys [MessageId ReceiptHandle]} messages]
                                                            {:Id                MessageId
                                                             :ReceiptHandle     ReceiptHandle
                                                             :VisibilityTimeout visibility-timeout})}}]
                         (aws/invoke client request)))
                     (doall))]
    {:Successful (vec (mapcat :Successful results))
     :Failed     (vec (mapcat :Failed results))}))

(defn ack-one [client {:keys [ReceiptHandle] :as message}]
  (let [queue-url (message->queue-url message)
        request   {:op      :DeleteMessage
                   :request {:QueueUrl      queue-url
                             :ReceiptHandle ReceiptHandle}}]
    (aws/invoke client request)))

(defn ack-many [client messages]
  (let [results (->> (for [[queue-url messages] (group-by message->queue-url messages)]
                       (let [request
                             {:op      :DeleteMessageBatch
                              :request {:QueueUrl queue-url
                                        :Entries  (for [{:keys [MessageId ReceiptHandle]} messages]
                                                    {:Id            MessageId
                                                     :ReceiptHandle ReceiptHandle})}}]
                         (aws/invoke client request)))
                     (doall))]
    {:Successful (vec (mapcat :Successful results))
     :Failed     (vec (mapcat :Failed results))}))

(defn nack-one [client message]
  (change-visibility-one client message 0))

(defn nack-many [client messages]
  (change-visibility-batch client messages 0))

