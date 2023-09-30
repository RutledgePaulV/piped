(ns piped.specs
  (:require [clojure.spec.alpha :as s]))

(s/def :piped/client-opts map?)
(s/def :piped/queue-url string?)
(s/def :piped/producer-parallelism pos-int?)
(s/def :piped/consumer-parallelism pos-int?)
(s/def :piped/acker-parallelism pos-int?)
(s/def :piped/nacker-parallelism pos-int?)
(s/def :piped/blocking-consumers boolean?)
(s/def :piped/message map?)
(s/def :piped/action #{:ack :nack})
(s/def :piped/extend #{:extend})
(s/def :piped/delay-seconds nat-int?)
(s/def :piped/action-map
  (s/keys
    :req-un [:piped/action]
    :opt-un [:piped/delay-seconds]))

(s/def :piped/consumer-fn ifn?)
(s/def :piped/transform-fn ifn?)
(s/def :piped/system any?)

(s/def :piped/options-map
  (s/keys
   :req-un [:piped/queue-url
            :piped/consumer-fn]
   :opt-un [:piped/client-opts
            :piped/producer-parallelism
            :piped/consumer-parallelism
            :piped/acker-parallelism
            :piped/nacker-parallelism
            :piped/blocking-consumers
            :piped/transform-fn]))

(defn assert-options [config]
  (if-not (s/valid? :piped/options-map config)
    (throw (ex-info "System options were invalid." {}))
    true))
