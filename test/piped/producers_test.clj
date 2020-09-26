(ns piped.producers-test
  (:require [clojure.test :refer :all]
            [piped.support :as support]
            [piped.producers :refer :all]
            [clojure.core.async :as async]))

(deftest basic-producer
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        producer   (async/chan)]
    (try
      (spawn-producer @support/client queue-url producer (support/dev-null))
      (is (nil? (async/poll! producer)))
      (let [message-id (support/send-message queue-url {:value 1})
            message    (async/<!! producer)]
        (is (= message-id (:MessageId message))))
      (finally
        (async/close! producer)))))