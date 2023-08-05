(ns piped.core-test
  (:require
   [clojure.edn :as edn]
   [clojure.test :refer :all]
   [clojure.tools.logging :as log]
   [piped.core :refer :all]
   [piped.support :as support]))

(use-fixtures :each (fn [tests] (stop-all-processors!) (tests) (stop-all-processors!)))

(def transform #(update % :Body edn/read-string))

(deftest basic-system-with-one-message
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        received   (promise)
        consumer   (fn [message] (deliver received message))
        system     (start (processor
                           {:consumer-fn  consumer
                            :queue-url    queue-url
                            :client-opts  (support/localstack-client-opts)
                            :transform-fn transform}))
        data       {:value 1}]
    (try
      (support/send-message queue-url data)
      (let [message (deref received 21000 :aborted)]
        (is (not= message :aborted))
        (is (= data (get message :Body))))
      (finally
        (stop system)))))

(deftest system-with-backoffs
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        received   (promise)
        backed-off (promise)
        nacker-fn  (fn [msg]
                     (let [attempts (some-> msg
                                            :Attributes
                                            (get "ApproximateReceiveCount" "0")
                                            (Integer/parseInt))]
                       (if (>= attempts 3)
                         (do
                           (deliver backed-off 10)
                           0)
                         3)))
        consumer (fn [{:keys [Body
                              Attributes] :as message}]
                   (deliver received message)
                   (condp = (:value Body)
                     1 :ack
                     2 (let [attempts (some-> Attributes
                                              (get "ApproximateReceiveCount" "0")
                                              (Integer/parseInt))]
                         (if (< attempts 4)
                           :nack
                           :ack))))
        system   (start (processor
                         {:consumer-fn  consumer
                          :queue-url    queue-url
                          :client-opts  (support/localstack-client-opts)
                          :transform-fn transform
                          :nacker-opts  {:visibility-timeout-fn nacker-fn}}))
        msgs     [{:value 1} {:value 2}]]
    (try
      (support/send-message-batch queue-url msgs)
      (let [message (deref received 21000 :aborted)
            backoff (deref backed-off 21000 0)]
        (is (not= message :aborted))
        (is (not= backoff 0))
        (is (= (first msgs)
               (get message :Body)))
        (is (= 10 backoff)))
      (finally
        (stop system)))))
