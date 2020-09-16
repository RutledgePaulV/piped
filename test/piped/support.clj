(ns piped.support
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds])
  (:import (org.testcontainers.containers.wait.strategy Wait)
           (org.testcontainers.containers GenericContainer)
           (java.time Duration)))

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

(defn localstack-client [client-opts]
  (delay
    (let [localstack-opts
          {:region
           "us-east-1"
           :credentials-provider
           (creds/basic-credentials-provider
             {:access-key-id     "localstack"
              :secret-access-key "localstack"})
           :endpoint-override
           {:protocol :http
            :hostname (.getContainerIpAddress @localstack)
            :port     (.getMappedPort @localstack 4576)}}]
      (aws/client (merge client-opts localstack-opts)))))

(def client (localstack-client {:api :sqs :http-client 'piped.http/create}))

(defn create-queue [queue-name]
  (let [op {:op      :CreateQueue
            :request {:QueueName queue-name}}]
    (:QueueUrl (aws/invoke @client op))))

(defn send-message [queue-url value]
  (let [op {:op      :SendMessage
            :request {:QueueUrl    queue-url
                      :MessageBody (pr-str value)}}]
    (:MessageId (aws/invoke @client op))))

(defn gen-queue-name []
  (name (gensym "piped-test-queue")))