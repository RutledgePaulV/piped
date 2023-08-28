(ns piped.extensions-test
  (:require
   [clojure.edn :as edn]
   [clojure.test :refer :all]
   [clojure.tools.logging :as log]
   [piped.core :as piped]
   [piped.support :as support])
  (:import
   [java.util.concurrent CountDownLatch TimeUnit]))

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
    (loop [waiting 25]
      (when-not (.await finished waiting TimeUnit/SECONDS)
        (is (empty? (support/receive-message-batch queue-url)))
        (recur 1)))
    (piped/stop processor)))

(def transform #(update % :Body edn/read-string))

(deftest configurable-visibility-timeouts
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        messages   [{:value 1} {:value 2}]
        finished   (CountDownLatch. (count messages))
        consumer   (fn [{:keys [Body
                                Attributes]}]
                     (condp = (:value Body)
                       1 (do (.countDown finished)
                             {:action :ack})
                       2 (let [attempts (some-> Attributes
                                                (get "ApproximateReceiveCount" "0")
                                                (Integer/parseInt))]
                           (if (< attempts 3)
                             {:action :nack :delay-seconds (rand-int 2)}
                             (do
                               (.countDown finished)
                               {:action :ack})))))
        processor  (piped/processor
                    {:queue-url            queue-url
                     :client-opts          (support/localstack-client-opts)
                     :consumer-fn          consumer
                     :consumer-parallelism 2
                     :transform-fn transform})]
    (support/send-message-batch queue-url messages)
    (piped/start processor)
    (loop [waiting 25]
      (when-not (.await finished waiting TimeUnit/SECONDS)
        (is (empty? (support/receive-message-batch queue-url)))
        (recur 1)))
    (piped/stop processor)))
