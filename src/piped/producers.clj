(ns piped.producers
  "Code relating to polling SQS messages from AWS and getting them onto channels."
  (:require [piped.utils :as utils]
            [clojure.core.async :as async]
            [cognitect.aws.client.api.async :as api.async]
            [clojure.tools.logging :as log]))


(defn spawn-producer
  ([client queue-url output-chan]
   (spawn-producer client queue-url output-chan (utils/dev-null)))

  ([client queue-url output-chan nack-chan]
   (spawn-producer client queue-url output-chan nack-chan {}))

  ([client queue-url output-chan nack-chan
    {:keys [MaxNumberOfMessages VisibilityTimeout MaxArtificialDelay]
     :or   {MaxNumberOfMessages 10 VisibilityTimeout 30 MaxArtificialDelay 60000}}]

   (let [close-chan (async/promise-chan)]

     (utils/on-chan-close output-chan (async/close! close-chan))

     (async/go-loop [max-number-of-messages MaxNumberOfMessages backoff-seq []]

       (log/debugf "Beginning new long poll of sqs queue %s." queue-url)

       (let [request
             {:op      :ReceiveMessage
              :request {:QueueUrl              queue-url
                        :MaxNumberOfMessages   max-number-of-messages
                        :VisibilityTimeout     VisibilityTimeout
                        :WaitTimeSeconds       utils/maximum-wait-time-seconds
                        :AttributeNames        ["All"]
                        :MessageAttributeNames ["All"]}}

             ; poll for messages
             response
             (async/alt!
               [(api.async/invoke client request)] ([v] v)
               ; if output-chan was closed while we're polling, abandon the poll.
               [close-chan] {:closed true})

             original-messages
             (get response :Messages [])

             deadline
             (async/timeout (- (* utils/visibility-timeout-seconds 1000) utils/deadline-safety-buffer))

             metadata
             {:deadline deadline :queue-url queue-url}

             messages-with-metadata
             (mapv #(with-meta % metadata) original-messages)

             [action remainder]
             (if (empty? messages-with-metadata)
               (cond
                 (utils/anomaly? response)
                 [:error []]
                 (true? (get response :closed))
                 [:closed []]
                 :else
                 [:empty []])
               (loop [[message :as messages] messages-with-metadata]
                 (if (seq messages)
                   (if-some [result
                             (async/alt!
                               [[output-chan message]]
                               ([val _] (if val ::accepted nil))
                               [deadline] ::timeout
                               :priority true)]
                     (if (= ::accepted result)
                       (recur (rest messages))
                       [:dead (into [] messages)])
                     [:closed (into [] messages)])
                   [:accepted []])))]

         (case action
           :error
           (let [backoffs (if (seq backoff-seq) backoff-seq (utils/backoff-seq MaxArtificialDelay))]
             (log/errorf "Error returned when polling sqs queue %s. Waiting for %d milliseconds." queue-url (first backoffs))
             (async/<! (async/timeout (first backoffs)))
             (recur max-number-of-messages (rest backoffs)))

           :empty
           (let [backoffs (if (seq backoff-seq) backoff-seq (utils/backoff-seq MaxArtificialDelay))]
             (log/debugf "Empty response when polling sqs queue %s. Waiting for %d milliseconds." queue-url (first backoffs))
             (async/<! (async/timeout (first backoffs)))
             (recur max-number-of-messages (rest backoffs)))

           :dead
           (let [wanted    max-number-of-messages
                 received  (count original-messages)
                 accepted  (- received (count remainder))
                 new-count (utils/clamp
                             utils/minimum-messages-received
                             utils/maximum-messages-received
                             (utils/average- accepted wanted))]
             (log/warnf "Consumers were unable to accept %d messages from %s before the messages expired." queue-url (count remainder))
             ; probably just let aws handle it since already very near expiry
             #_(async/onto-chan! nack-chan remainder false)
             (recur new-count []))

           :closed
           (do (log/debugf "Producer stopping because channel for queue %s has been closed." queue-url)
               (async/<! (async/onto-chan! nack-chan remainder false))
               :complete)

           :accepted
           (let [received  (count original-messages)
                 new-count (utils/clamp
                             utils/minimum-messages-received
                             utils/maximum-messages-received
                             (utils/average+ received utils/maximum-messages-received))]
             (log/debugf "All messages polled from %s were accepted by consumers." queue-url)
             (recur new-count []))))))))