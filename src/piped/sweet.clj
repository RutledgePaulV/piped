(ns piped.sweet
  (:require [piped.core :as piped]
            [cognitect.aws.client.api :as aws]
            [clojure.tools.logging :as log]))



(comment
  (defmacro defprocessor [symbol bindings attributes & body]
    `(let [client# (aws/client {:api :sqs})
           attrs#  ~attributes
           var#    (defmulti ~symbol (fn ~bindings ~@body))
           system# (delay (piped/spawn-system client# (get attrs# :queue-url) var# attrs#))]
       (with-meta var# {:system system#})))

  (defprocessor queue-processor [message]
                {:queue-url            "http://localhost:4576/queue/piped-test-queue17460"
                 :consumer-parallelism 10
                 :producer-parallelism 2
                 :transform            identity}
                (get message :kind))

  (defmethod queue-processor :alert [message]
    )

  (defmethod queue-processor :alert [message]
    )
  )