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
        (.setEnv [(str "HOSTNAME_EXTERNAL=" (.getContainerIpAddress container))])
        (.withStartupTimeout (Duration/ofMinutes 1))
        (.waitingFor (Wait/forLogMessage "^Ready\\.\\s*$" 1))
        (.start)))))

(defn localstack-client [client-opts]
  (delay
    (let [localstack-opts
          {:credentials-provider
           (creds/basic-credentials-provider
             {:access-key-id     "localstack"
              :secret-access-key "localstack"})
           :endpoint-override
           {:protocol :http
            :hostname "localhost"
            :port     (.getMappedPort @localstack 4576)}}]
      (aws/client (merge client-opts localstack-opts)))))