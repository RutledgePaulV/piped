(ns piped.sqs
  "Functions relating to interacting with SQS."
  (:require [cognitect.aws.client.api :as aws]
            [piped.utils :as utils]))

(defn- combine-batch-results [results]
  {:Successful (vec (mapcat :Successful results))
   :Failed     (vec (mapcat :Failed results))})

(defn change-visibility-one [client {:keys [ReceiptHandle] :as message} visibility-timeout]
  (let [request {:op      :ChangeMessageVisibility
                 :request {:QueueUrl          (utils/message->queue-url message)
                           :ReceiptHandle     ReceiptHandle
                           :VisibilityTimeout visibility-timeout}}]
    (aws/invoke client request)))

(defn change-visibility-batch [client messages visibility-timeout]
  (->> (for [[queue-url messages] (group-by utils/message->queue-url messages)]
         (let [request {:op      :ChangeMessageVisibilityBatch
                        :request {:QueueUrl queue-url
                                  :Entries  (for [{:keys [MessageId ReceiptHandle]} messages]
                                              {:Id                MessageId
                                               :ReceiptHandle     ReceiptHandle
                                               :VisibilityTimeout visibility-timeout})}}]
           (aws/invoke client request)))
       (combine-batch-results)))

(defn ack-one [client {:keys [ReceiptHandle] :as message}]
  (let [queue-url (utils/message->queue-url message)
        request   {:op      :DeleteMessage
                   :request {:QueueUrl      queue-url
                             :ReceiptHandle ReceiptHandle}}]
    (aws/invoke client request)))

(defn ack-many [client messages]
  (->> (for [[queue-url messages] (group-by utils/message->queue-url messages)]
         (let [request
               {:op      :DeleteMessageBatch
                :request {:QueueUrl queue-url
                          :Entries  (for [{:keys [MessageId ReceiptHandle]} messages]
                                      {:Id            MessageId
                                       :ReceiptHandle ReceiptHandle})}}]
           (aws/invoke client request)))
       (combine-batch-results)))

(defn nack-one [client message]
  (change-visibility-one client message 0))

(defn nack-many [client messages]
  (change-visibility-batch client messages 0))

