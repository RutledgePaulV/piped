(ns piped.core
  "The public API."
  (:require [clojure.core.async :as async]
            [piped.consumers :as consumers]
            [piped.producers :as producers]))

; system registry
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
  ([client queue-url consumer-fn]
   (spawn-system client queue-url consumer-fn {}))
  ([client queue-url consumer-fn
    {:keys [producer-parallelism consumer-parallelism blocking-consumers pipe]
     :or   {producer-parallelism 1
            consumer-parallelism 5
            blocking-consumers   true
            pipe                 (async/chan 10)}}]

   (letfn [(spawn-producer []
             (producers/spawn-producer client queue-url pipe))

           (spawn-consumer []
             (if blocking-consumers
               (consumers/spawn-consumer-blocking client pipe consumer-fn)
               (consumers/spawn-consumer-compute client pipe consumer-fn)))]

     (let [producers (doall (repeatedly producer-parallelism spawn-producer))
           consumers (doall (repeatedly consumer-parallelism spawn-consumer))
           shutdown  (fn []
                       ; remove it from the registry, it's coming down
                       (swap! systems dissoc queue-url)
                       ; stop the flow of data from producers to consumers
                       (async/close! pipe)
                       ; wait for producers to exit
                       (run! async/<!! producers)
                       ; wait for consumers to exit
                       (run! async/<!! consumers))]

       (let [[old] (swap-vals! systems assoc queue-url shutdown)]
         (when-some [shutdown-for-old-system (get old queue-url)]
           (shutdown-for-old-system)))

       shutdown))))


