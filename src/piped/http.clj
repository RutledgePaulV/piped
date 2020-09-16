(ns piped.http
  (:require [cognitect.http-client :as impl]
            [cognitect.aws.http :as aws]
            [clojure.tools.logging :as log]))


(defn create
  []
  (let [opts {:trust-all                       true
              :pending-ops-limit               1000
              :max-connections-per-destination 1000}
        c    (impl/create opts)]
    (reify aws/HttpClient
      (-submit [_ request channel]
        (log/info "request!" (pr-str request))
        (impl/submit c request channel))
      (-stop [_]
        (impl/stop c)))))