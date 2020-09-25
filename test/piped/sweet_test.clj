(ns piped.sweet-test
  (:require [clojure.test :refer :all])
  (:require [piped.sweet :refer :all]
            [piped.core :as piped]
            [piped.support :as support]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]))

(defn messages []
  (interleave (repeat {:kind :alert :message "The building is on fire!"})
              (repeat {:kind :warn :message "You better do your homework!"})))

(comment
  (def queue-url
    (support/create-queue (support/gen-queue-name)))

  (defmultiprocessor my-processor [{:keys [Body]}]
    {:queue-url            queue-url
     :consumer-parallelism 1000
     :client-opts          (support/localstack-client-opts)
     :transform            #(update % :Body edn/read-string)}
    (get Body :kind))

  (defmethod my-processor :alert [{{:keys [message]} :Body}]
    (log/error message))

  (defmethod my-processor :warn [{{:keys [message]} :Body}]
    (log/warn message))

  (dotimes [_ 100]
    (support/send-message-batch queue-url (take 10 (messages))))

  (piped/start #'my-processor)

  (piped/stop #'my-processor))

