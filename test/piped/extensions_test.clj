(ns piped.extensions-test
  (:require [clojure.test :refer :all]
            [piped.core :as piped]
            [piped.support :as support]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))


(deftest visibility-timeouts-are-extended
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        messages   [{:message 1} {:message 2}]
        finished   (CountDownLatch. (count messages))
        consumer   (fn [msg]
                     (log/info "Received message" (pr-str msg))
                     (Thread/sleep 40000)
                     (.countDown finished)
                     :ack)
        processor  (piped/processor
                     {:queue-url            queue-url
                      :client-opts          (support/localstack-client-opts)
                      :consumer-fn          consumer
                      :consumer-parallelism 2})]
    (support/send-message-batch queue-url messages)
    (piped/start processor)
    (loop [waiting 31]
      (when-not (.await finished waiting TimeUnit/SECONDS)
        (is (empty? (support/receive-message-batch queue-url)))
        (recur 1)))
    (piped/stop processor)))