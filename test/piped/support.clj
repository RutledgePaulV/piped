(ns piped.support
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as creds]
            [clojure.pprint :as pprint])
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
      (pprint/pprint localstack-opts)
      (aws/client (merge client-opts localstack-opts)))))