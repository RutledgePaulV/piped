(ns piped.core
  "The public API."
  (:require [clojure.core.async :as async]
            [piped.consumers :as consumers]
            [piped.producers :as producers]
            [piped.actions :as actions]
            [piped.utils :as utils]
            [cognitect.aws.client.api :as aws]
            [clojure.edn :as edn]))

(defprotocol PipedSystem
  :extend-via-metadata true
  (start [this] "Start polling SQS and processing messages.")
  (stop [this] "Stop the system and await completion of in-flight messages."))

; system registry
(defonce systems (atom {}))

(defn stop-system
  "For a given queue-url, stop the associated system (if any)."
  [queue-url]
  (when-some [system (get @systems queue-url)]
    (stop system)))

(defn start-system
  "For a given queue-url, start the associated system (if any)."
  [queue-url]
  (when-some [system (get @systems queue-url)]
    (start system)))

(defn start-all-systems
  "Stop all running systems."
  []
  (run! stop (vals @systems)))

(defn stop-all-systems
  "Stop all running systems."
  []
  (run! start (vals @systems)))

(defn default-client []
  (aws/client {:api :sqs}))

(defn default-transform [message]
  (update message :Body edn/read-string))

(defn create-system
  "Spawns a set of producers and consumers for a given queue.

   Returns a function of no arguments that can be called to stop the system."
  ([{:keys [client queue-url consumer-fn] :as attrs}]
   (create-system client queue-url consumer-fn (dissoc attrs :client :queue-url :consumer-fn)))

  ([client queue-url consumer-fn]
   (create-system client queue-url consumer-fn {}))

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
            blocking-consumers   true}}]

   (let [client         (or client (default-client))
         transform      (or transform default-transform)
         acker-chan     (async/chan 10)
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

       (let [processes
             (delay {:producers (doall (repeatedly producer-parallelism spawn-producer))
                     :consumers (doall (repeatedly consumer-parallelism spawn-consumer))
                     :ackers    (doall (repeatedly acker-parallelism spawn-acker))
                     :nackers   (doall (repeatedly nacker-parallelism spawn-nacker))})

             shutdown-thread
             (Thread.
               ^Runnable
               (fn []
                 (when (realized? processes)
                   (let [{:keys [producers consumers ackers nackers]} (force processes)]
                     ; signal producers and consumers
                     (async/close! pipe)
                     ; wait for producers to exit
                     (run! async/<!! producers)
                     ; wait for consumers to exit
                     (run! async/<!! consumers)
                     ; signal ackers
                     (async/close! acker-chan)
                     ; wait for ackers to exit
                     (run! async/<!! ackers)
                     ; signal nackers
                     (async/close! nacker-chan)
                     ; wait for nackers to exit
                     (run! async/<!! nackers)))))

             system
             (reify PipedSystem
               (start [this]
                 (when-not (realized? processes)
                   (.addShutdownHook (Runtime/getRuntime) shutdown-thread)
                   (force processes))
                 this)
               (stop [this]
                 (when (realized? processes)
                   (.removeShutdownHook (Runtime/getRuntime) shutdown-thread)
                   (.run shutdown-thread))
                 this))]

         (swap! systems assoc queue-url system)

         system)))))




