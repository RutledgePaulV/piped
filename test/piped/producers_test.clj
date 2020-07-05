(ns piped.producers-test
  (:require [clojure.test :refer :all]
            [piped.support :as support]
            [piped.producers :refer :all]
            [cognitect.aws.client.api :as aws]
            [clojure.core.async :as async]))

(defonce sqs (support/localstack-client {:api :sqs}))

(defn create-queue [queue-name]
  (let [op {:op      :CreateQueue
            :request {:QueueName queue-name}}]
    (:QueueUrl (aws/invoke @sqs op))))

(defn send-message [queue-url value]
  (let [op {:op      :SendMessage
            :request {:QueueUrl    queue-url
                      :MessageBody (pr-str value)}}]
    (:MessageId (aws/invoke @sqs op))))

(defn gen-queue-name []
  (name (gensym "piped-test-queue")))

(deftest basic-producer
  (let [queue-name
        (gen-queue-name)
        queue-url
        (create-queue queue-name)
        producer
        (async/chan)]
    (try
      (spawn-producer @sqs queue-url producer)
      (is (nil? (async/poll! producer)))
      (let [message-id (send-message queue-url {:value 1})
            message    (async/<!! producer)]
        (is (= message-id (:MessageId message))))
      (finally
        (async/close! producer)))))