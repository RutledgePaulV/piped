(ns piped.producers
  "Code relating to polling SQS messages from AWS and getting them onto channels."
  (:require [piped.utils :as utils]
            [clojure.core.async :as async]
            [cognitect.aws.client.api.async :as api.async]
            [clojure.tools.logging :as log]
            [clojure.core.async.impl.protocols :as impl]))

(def min-receive 1)
(def max-receive 10)
(def max-wait 20)
(def initial-timeout-seconds 30)

(defn spawn-producer
  ([client queue-url output-chan nack-chan]
   (spawn-producer client queue-url output-chan nack-chan {}))

  ([client queue-url output-chan nack-chan
    {:keys [MaxNumberOfMessages VisibilityTimeout]
     :or   {MaxNumberOfMessages max-receive VisibilityTimeout initial-timeout-seconds}}]

   (async/go-loop [max-number-of-messages MaxNumberOfMessages backoff-seq []]

     (log/debugf "Beginning new long poll of sqs queue %s." queue-url)

     (let [request
           {:op      :ReceiveMessage
            :request {:QueueUrl              queue-url
                      :MaxNumberOfMessages   max-number-of-messages
                      :VisibilityTimeout     VisibilityTimeout
                      :WaitTimeSeconds       max-wait
                      :AttributeNames        ["All"]
                      :MessageAttributeNames ["All"]}}

           ; poll for messages
           response
           (async/<! (api.async/invoke client request))

           original-messages
           (get response :Messages [])

           deadline
           (async/timeout (- (* VisibilityTimeout 1000) utils/buffer-millis))

           metadata
           {:deadline deadline :queue-url queue-url :timeout VisibilityTimeout}

           messages-with-metadata
           (mapv #(with-meta % metadata) original-messages)

           [action remainder]
           (if (empty? messages-with-metadata)
             (cond (utils/anomaly? response)
                   [:error []]
                   (impl/closed? output-chan)
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
                     [:expired (into [] messages)])
                   [:closed (into [] messages)])
                 [:accepted []])))]

       (case action
         :error
         (let [[backoff :as backoff-seq] (or (seq backoff-seq) (utils/backoff-seq))]
           (log/errorf "Error returned when polling sqs queue %s. Backing off %d milliseconds. %s" queue-url backoff (pr-str response))
           (async/<! (async/timeout backoff))
           (recur max-number-of-messages (rest backoff-seq)))

         :empty
         (recur max-number-of-messages [])

         :expired
         (let [wanted    max-number-of-messages
               received  (count original-messages)
               accepted  (- received (count remainder))
               new-count (utils/clamp min-receive max-receive (utils/average- accepted wanted))]
           (log/warnf "Consumers of %s unable to accept %d messages before they expired." queue-url (count remainder))
           ; probably just let aws handle it and skip the network traffic since already very near expiry
           #_(async/onto-chan! nack-chan remainder false)
           (recur new-count []))

         :closed
         (do (log/debugf "Producer stopping because channel for queue %s has been closed." queue-url)
             (async/<! (async/onto-chan! nack-chan remainder false))
             :complete)

         :accepted
         (let [received  (count original-messages)
               new-count (utils/clamp min-receive max-receive (utils/average+ received max-receive))]
           (log/debugf "All messages polled from %s were accepted by consumers." queue-url)
           (recur new-count [])))))))
