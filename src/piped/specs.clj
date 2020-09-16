(ns piped.specs
  (:require [clojure.spec.alpha :as s]))

(s/def :piped/client any?)
(s/def :piped/queue-url string?)
(s/def :piped/producer-parallelism pos-int?)
(s/def :piped/consumer-parallelism pos-int?)
(s/def :piped/acker-parallelism pos-int?)
(s/def :piped/nacker-parallelism pos-int?)
(s/def :piped/blocking-consumers boolean?)
(s/def :piped/message map?)
(s/def :piped/action #{:ack :nack})
(s/def :piped/consumer-fn (s/fspec :args (s/cat :message :piped/message) :ret :piped/action))
(s/def :piped/transformer (s/fspec :args (s/cat :message :piped/message) :ret :piped/message))
(s/def :piped/system any?)

(s/def :piped/options-map
  (s/keys
    :req-un [:piped/queue-url]
    :opt-un [:piped/client
             :piped/producer-parallelism
             :piped/consumer-parallelism
             :piped/acker-parallelism
             :piped/nacker-parallelism
             :piped/blocking-consumers
             :piped/consumer-fn]))

(s/def :piped/extras-map
  (s/keys :opt-un [:piped/producer-parallelism
                   :piped/consumer-parallelism
                   :piped/acker-parallelism
                   :piped/nacker-parallelism
                   :piped/blocking-consumers]))

(s/fdef
  piped.core/create-system
  :args (s/alt
          :unary (s/cat :piped/options-map)
          :tertiary (s/cat :piped/client :piped/queue-url :piped/consumer-fn)
          :quadrinary (s/cat :piped/client :piped/queue-url :piped/consumer-fn :piped/extras-map))
  :ret :piped/system)