(ns piped.core
  "The public API."
  (:require [clojure.core.async :as async]
            [piped.consumers :as consumers]
            [piped.producers :as producers]
            [piped.actions :as actions]
            [piped.utils :as utils]))

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

    {:keys [producer-parallelism
            consumer-parallelism
            acker-parallelism
            nacker-parallelism
            blocking-consumers
            transform]

     :or   {producer-parallelism 1
            consumer-parallelism 10
            acker-parallelism    2
            nacker-parallelism   2
            blocking-consumers   true
            transform            identity}}]

   (let [acker-chan     (async/chan 10)
         nacker-chan    (async/chan 10)
         pipe           (async/chan)
         transformed    (async/map transform [pipe])
         acker-batched  (utils/deadline-batching acker-chan 10 utils/message->deadline)
         nacker-batched (utils/batching nacker-chan 5000 10)]

     (letfn [(spawn-producer []
               (let [opts {:MaxNumberOfMessages (min 10 consumer-parallelism)}]
                 (producers/spawn-producer client queue-url pipe nacker-chan opts)))

             (spawn-consumer []
               (if blocking-consumers
                 (consumers/spawn-consumer-blocking client transformed acker-chan nacker-chan consumer-fn)
                 (consumers/spawn-consumer-compute client transformed acker-chan nacker-chan consumer-fn)))

             (spawn-acker []
               (actions/spawn-acker client acker-batched))

             (spawn-nacker []
               (actions/spawn-nacker client nacker-batched))]

       (let [producers (doall (repeatedly producer-parallelism spawn-producer))
             consumers (doall (repeatedly consumer-parallelism spawn-consumer))
             ackers    (doall (repeatedly acker-parallelism spawn-acker))
             nackers   (doall (repeatedly nacker-parallelism spawn-nacker))
             shutdown  (fn [internal?]
                         ; remove it from the registry, it's coming down
                         (when-not internal?
                           (swap! systems dissoc queue-url))
                         ; stop the flow of data from producers to consumers
                         (async/close! pipe)
                         ; wait for producers to exit
                         (run! async/<!! producers)
                         ; wait for consumers to exit
                         (run! async/<!! consumers)
                         ; wait for ackers to exit
                         (async/close! acker-chan)
                         (run! async/<!! ackers)
                         ; wait for nackers to exit
                         (async/close! nacker-chan)
                         (run! async/<!! nackers))]

         (let [[old] (swap-vals! systems assoc queue-url shutdown)]
           (when-some [shutdown-for-old-system (get old queue-url)]
             (shutdown-for-old-system true)))

         (fn [] (shutdown false)))))))


