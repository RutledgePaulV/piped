(ns piped.actions-test
  (:require [clojure.test :refer :all])
  (:require [piped.actions :refer [spawn-acker]]
            [piped.producers :refer [spawn-producer]]
            [piped.support :as support]
            [clojure.core.async :as async]))


(deftest spawn-acker-test
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        producer   (async/chan)
        acker-loop (spawn-acker @support/client producer)]
    (dotimes [_ 5]
      (support/send-message queue-url {:value 1}))
    (let [producer-loop (spawn-producer @support/client queue-url producer)]
      (async/<!! (async/timeout 5000))
      (async/close! producer)
      (async/<!! acker-loop)
      (async/<!! producer-loop))))
