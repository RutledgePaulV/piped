(ns piped.core-test
  (:require [clojure.test :refer :all]
            [piped.core :refer :all]
            [clojure.edn :as edn]
            [piped.support :as support]
            [clojure.core.async :as async]))

(use-fixtures :each (fn [tests] (stop-all-systems) (tests) (stop-all-systems)))

(def xform (map #(update % :Body edn/read-string)))

(deftest basic-system-with-one-message
  (let [queue-name (support/gen-queue-name)
        queue-url  (support/create-queue queue-name)
        received   (promise)
        consumer   (fn [message] (deliver received message))
        pipe       (async/chan 10 xform)
        shutdown   (spawn-system @support/client queue-url consumer {:pipe pipe})
        data       {:value 1}]
    (try
      (support/send-message queue-url data)
      (let [message (deref received 20000 :aborted)]
        (is (not= message :aborted))
        (is (= data (get message :Body))))
      (finally
        (shutdown)))))