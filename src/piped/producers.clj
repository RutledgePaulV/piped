(ns piped.producers
  "Code relating to polling SQS messages from AWS and getting them onto channels."
  (:require [piped.utils :as utils]
            [piped.sqs :as sqs]
            [clojure.core.async :as async]
            [cognitect.aws.client.api :as aws]))


(defn spawn-producer
  ([client queue-url return-chan]
   (spawn-producer client queue-url return-chan {}))

  ([client
    queue-url
    return-chan
    {:keys [MaxNumberOfMessages VisibilityTimeout]
     :or   {MaxNumberOfMessages 10 VisibilityTimeout 30}}]

   ; always start with a short poll, then we'll adjust
   ; our rate over time to align with the producer
   ; and we'll never exceed the rate of the consumer
   ; thanks to channel buffer backpressure
   (async/go-loop [WaitTimeSeconds 0]

     (if @(.closed return-chan)

       true

       (let [request
             {:op      :ReceiveMessage
              :request {:QueueUrl              queue-url
                        :MaxNumberOfMessages   MaxNumberOfMessages
                        :VisibilityTimeout     VisibilityTimeout
                        :WaitTimeSeconds       WaitTimeSeconds
                        :AttributeNames        ["All"]
                        :MessageAttributeNames ["All"]}}

             ; poll for messages
             {:keys [Messages] :or {Messages []}}
             (async/<! (async/thread (aws/invoke client request)))

             abandoned
             (loop [[message :as messages] Messages]
               (if (not-empty messages)
                 (if (async/>! return-chan message)
                   (recur (rest messages))
                   messages)
                 []))]

         ; channel was closed with some stragglers, nack them so other workers can begin asap.
         (if (not-empty abandoned)

           (do (async/<! (async/thread (sqs/nack-many client queue-url abandoned))) true)

           (cond
             ; this set was empty, begin backing off the throttle
             (empty? Messages)
             (recur (utils/bounded-inc WaitTimeSeconds 20))

             ; this round was neither empty nor full, stay the course
             (< 0 (count Messages) MaxNumberOfMessages)
             (recur WaitTimeSeconds)

             ; this round was full, hit the gas!
             (= (count Messages) MaxNumberOfMessages)
             (recur (utils/bounded-dec WaitTimeSeconds 0)))))))))