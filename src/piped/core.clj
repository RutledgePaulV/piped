(ns piped.core
  "The public API."
  (:require [cognitect.aws.client.api :as aws]
            [clojure.core.async :as async]
            [piped.consumers :as consumers]
            [piped.producers :as producers]))

(defonce systems (atom {}))

(defn stop-system
  "For a given queue-url, stop the associated system (if any)."
  [key]
  (let [[old] (swap-vals! systems dissoc key)]
    (some-> old (get key) (async/close!))))

(defn stop-all-systems
  "Stop all running systems."
  []
  (doseq [v (vals (first (reset-vals! systems {})))]
    (async/close! v)))

(defn spawn-system
  "Spawns a set of producers and consumers for a given queue.

   Returns a function of no arguments that can be called to stop the system."
  ([queue-url consumer-fn]
   (spawn-system queue-url consumer-fn {}))
  ([queue-url consumer-fn
    {:keys [client blocking producer-n consumer-n pipe]
     :or   {producer-n 1
            consumer-n 1
            blocking   true
            pipe       (async/chan 10)}
     :as   options}]
   (dotimes [_ producer-n]
     (producers/spawn-producer client queue-url pipe))
   (dotimes [_ consumer-n]
     (if-not blocking
       (consumers/spawn-consumer client pipe consumer-fn)
       (consumers/spawn-consumer-blocking client pipe consumer-fn)))
   (fn [] (async/close! pipe))))


