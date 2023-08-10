(ns piped.sqs
  "Functions relating to interacting with SQS."
  (:require
   [clojure.core.async :as async]
   [clojure.tools.logging :as log]
   [cognitect.aws.client.api.async :as api.async]
   [piped.utils :as utils]))

(defn- combine-batch-results [result-chans]
  (if (= 1 (count result-chans))
    (first result-chans)
    (async/go-loop [channels (set result-chans)
                    results {:Successful [] :Failed []}]
      (if (empty? channels)
        results
        (let [[value port] (async/alts! (vec channels))]
          (recur
           (disj channels port)
           (-> results
               (update :Successful #(into % (:Successful value [])))
               (update :Failed #(into % (:Failed value []))))))))))

(defn change-visibility-one [client {:keys [ReceiptHandle] :as message} visibility-timeout]
  (let [request {:op      :ChangeMessageVisibility
                 :request {:QueueUrl          (utils/message->queue-url message)
                           :ReceiptHandle     ReceiptHandle
                           :VisibilityTimeout visibility-timeout}}]
    (log/info request)
    (api.async/invoke client request)))

(defn change-visibility-batch
  ([client messages visibility-timeout]
   (->> (for [[queue-url msgs] (group-by utils/message->queue-url messages)]
          (let [request {:op      :ChangeMessageVisibilityBatch
                         :request {:QueueUrl queue-url
                                   :Entries  (for [{:keys [MessageId ReceiptHandle] :as msg}
                                                   msgs]
                                               {:Id                MessageId
                                                :ReceiptHandle     ReceiptHandle
                                                :VisibilityTimeout (or (utils/message->timeout msg)
                                                                       visibility-timeout)})}}]
            (log/info request)
            (api.async/invoke client request)))
        (combine-batch-results))))

(defn ack-many [client messages]
  (->> (for [[queue-url messages] (group-by utils/message->queue-url messages)]
         (let [request
               {:op      :DeleteMessageBatch
                :request {:QueueUrl queue-url
                          :Entries  (for [{:keys [MessageId ReceiptHandle]} messages]
                                      {:Id            MessageId
                                       :ReceiptHandle ReceiptHandle})}}]
           (api.async/invoke client request)))
       (combine-batch-results)))

(defn nack-many
  ([client messages]
   (change-visibility-batch client messages 0)))
