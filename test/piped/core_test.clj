(ns piped.core-test
  (:require [clojure.test :refer :all]
            [piped.core :refer :all]
            [clojure.edn :as edn]
            [piped.support :as support]))

(use-fixtures :each (fn [tests] (stop-all-systems) (tests) (stop-all-systems)))

(def transform #(update % :Body edn/read-string))

(deftest basic-system-with-one-message
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        received   (promise)
        consumer   (fn [message] (deliver received message))
        system     (start (create-system @support/client queue-url consumer {:transform transform}))
        data       {:value 1}]
    (try
      (support/send-message queue-url data)
      (let [message (deref received 21000 :aborted)]
        (is (not= message :aborted))
        (is (= data (get message :Body))))
      (finally
        (stop system)))))