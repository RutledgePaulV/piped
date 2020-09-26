(ns piped.support
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [clojure.core.async :as async])
  (:import (org.testcontainers.containers.wait.strategy Wait)
           (org.testcontainers.containers GenericContainer)
           (java.time Duration)
           [java.util UUID]))

(defonce localstack
  (delay
    (let [container (GenericContainer. "localstack/localstack:0.10.9")]
      (doto container
        (.setExposedPorts [(int 4576)])
        (.setEnv ["SERVICES=sqs" (str "HOSTNAME_EXTERNAL=" (.getContainerIpAddress container))])
        (.withStartupTimeout (Duration/ofMinutes 1))
        (.waitingFor (Wait/forLogMessage "^Ready\\.\\s*$" 1))
        (.start))
      container)))

(defn localstack-client-opts []
  {:api    :sqs
   :region "us-east-1"
   :credentials-provider
           (creds/basic-credentials-provider
             {:access-key-id     "localstack"
              :secret-access-key "localstack"})
   :endpoint-override
           {:protocol :http
            :hostname (.getContainerIpAddress @localstack)
            :port     (.getMappedPort @localstack 4576)}})

(def client
  (delay (aws/client (localstack-client-opts))))

(defn create-queue [queue-name]
  (let [op {:op      :CreateQueue
            :request {:QueueName queue-name}}]
    (:QueueUrl (aws/invoke @client op))))

(defn send-message [queue-url value]
  (let [op {:op      :SendMessage
            :request {:QueueUrl    queue-url
                      :MessageBody (pr-str value)}}]
    (:MessageId (aws/invoke @client op))))

(defn receive-message-batch [queue-url]
  (let [op {:op      :ReceiveMessage
            :request {:QueueUrl              queue-url
                      :MaxNumberOfMessages   10
                      :VisibilityTimeout     5
                      :WaitTimeSeconds       0
                      :AttributeNames        ["All"]
                      :MessageAttributeNames ["All"]}}]
    (:Messages (aws/invoke @client op) [])))

(defn send-message-batch [queue-url messages]
  (let [op {:op      :SendMessageBatch
            :request {:QueueUrl queue-url
                      :Entries  (for [message messages]
                                  {:Id          (str (UUID/randomUUID))
                                   :MessageBody message})}}]
    (aws/invoke @client op)))

(defn gen-queue-name []
  (name (gensym "piped-test-queue")))

(defn dev-null []
  (async/chan (async/dropping-buffer 0)))