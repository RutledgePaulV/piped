(ns piped.sweet-test
  (:require [clojure.test :refer :all])
  (:require [piped.sweet :refer [defprocessor]]
            [piped.core :as piped]
            [piped.support :as support]))


(comment
  (def queue-url
    (support/create-queue (support/gen-queue-name)))

  (defprocessor my-processor [{:keys [Body]}]
    {:queue-url queue-url
     :client    (force support/client)}
    (get Body :kind))

  (defmethod my-processor :alert [{:keys [Body]}]
    (println "Alert!" (get Body :message))
    (println (.getName (Thread/currentThread))))

  (defmethod my-processor :warn [{:keys [Body]}]
    (println "Warning!" (get Body :message))
    (println (.getName (Thread/currentThread))))

  (piped/start #'my-processor)
  (support/send-message queue-url {:kind :alert :message "The building is on fire!"})
  (support/send-message queue-url {:kind :warn :message "You better do your homework!"})
  (piped/stop #'my-processor))
