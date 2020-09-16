(ns piped.http
  (:require [cognitect.http-client :as impl]
            [cognitect.aws.http :as aws]))


(defn create
  []
  (let [c (impl/create {:trust-all          true
                        :pending-opts-limit 1000})]
    (reify aws/HttpClient
      (-submit [_ request channel]
        (impl/submit c request channel))
      (-stop [_]
        (impl/stop c)))))