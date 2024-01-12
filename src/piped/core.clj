(ns piped.core
  "The public API."
  (:require [clojure.core.async :as async]
            [piped.consumers :as consumers]
            [piped.producers :as producers]
            [piped.actions :as actions]
            [piped.utils :as utils]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.http :as http]
            [cognitect.http-client :as impl]
            [piped.specs :as specs]
            [clojure.tools.logging :as log]))

(defprotocol PipedProcessor
  :extend-via-metadata true
  (start [this] "Start polling SQS and processing messages.")
  (stop [this] "Stop the system and await completion of in-flight messages.")
  (running? [this] "Is the system currently running?"))

; registry
(defonce processors (atom {}))

(defn get-all-processors
  "Returns all registered systems in no particular order."
  []
  (or (vals @processors) ()))

(defn get-processor-by-queue-url
  "Gets the system for a given queue url. Returns nil if there is no such system."
  [queue-url]
  (get @processors queue-url))

(defn start-processor-by-queue-url!
  "For a given queue-url, start the associated system (if any)."
  [queue-url]
  (some-> queue-url (get-processor-by-queue-url) (start)))

(defn stop-processor-by-queue-url!
  "For a given queue-url, stop the associated system (if any)."
  [queue-url]
  (some-> queue-url (get-processor-by-queue-url) (stop)))

(defn start-all-processors!
  "Stop all running systems."
  []
  (run! start (get-all-processors)))

(defn stop-all-processors!
  "Stop all running systems. Systems are stopped concurrently
   for a faster return but this function blocks until they have
   all been fully shutdown."
  []
  (run! deref (doall (map #(future (stop %)) (get-all-processors)))))

(defn http-client
  "Returns a http client using cognitect's async jetty client wrapper."
  [http-opts]
  (let [c (impl/create http-opts)]
    (reify http/HttpClient
      (-submit [_ request channel]
        (impl/submit c request channel))
      (-stop [_]
        (impl/stop c)))))

(defn processor
  "Spawns a set of producers and consumers for a given queue.

   Returns an implementation of the PipedProcessor protocol which
   represents a message processing machine that can be started and
   stopped."
  [{:keys [client-opts
           queue-url
           consumer-fn
           transform-fn
           producer-parallelism
           consumer-parallelism
           acker-parallelism
           nacker-parallelism
           blocking-consumers
           queue-visibility-timeout-seconds]
    :as   opts}]

  (specs/assert-options opts)

  (letfn [(launch []
            (let [consumer-parallelism (or consumer-parallelism 10)
                  blocking-consumers   (if (boolean? blocking-consumers) blocking-consumers true)
                  producer-parallelism (or producer-parallelism (utils/quot+ consumer-parallelism 10))
                  acker-parallelism    (or acker-parallelism producer-parallelism)
                  nacker-parallelism   (or nacker-parallelism producer-parallelism)
                  max-http-ops         (+ producer-parallelism acker-parallelism nacker-parallelism)
                  http-client          (delay (http-client
                                                {:pending-ops-limit               max-http-ops
                                                 :max-connections-per-destination max-http-ops}))
                  client               (cond-> (or client-opts {})
                                         (not (contains? client-opts :http-client))
                                         (assoc :http-client (force http-client))
                                         :always
                                         (assoc :api :sqs)
                                         :always
                                         (aws/client))
                  acker-chan           (async/chan)
                  nacker-chan          (async/chan)
                  pipe                 (async/chan)
                  acker-batched        (utils/deadline-batching acker-chan 10)
                  nacker-batched       (utils/combo-batching nacker-chan 5000 10)
                  composed-consumer    (if transform-fn (comp consumer-fn transform-fn) consumer-fn)]

              (letfn [(spawn-producer []
                        (let [opts {:MaxNumberOfMessages (min 10 consumer-parallelism)
                                    :VisibilityTimeout   (or queue-visibility-timeout-seconds
                                                             producers/initial-timeout)}]
                          (producers/spawn-producer client queue-url pipe nacker-chan opts)))

                      (spawn-consumer []
                        (if blocking-consumers
                          (consumers/spawn-consumer-blocking client pipe acker-chan nacker-chan composed-consumer)
                          (consumers/spawn-consumer-async client pipe acker-chan nacker-chan composed-consumer)))

                      (spawn-acker []
                        (actions/spawn-acker client acker-batched))

                      (spawn-nacker []
                        (actions/spawn-nacker client nacker-batched))]

                {:client         client
                 :acker-chan     acker-chan
                 :nacker-chan    nacker-chan
                 :pipe           pipe
                 :acker-batched  acker-batched
                 :nacker-batched nacker-batched
                 :producers      (doall (repeatedly producer-parallelism spawn-producer))
                 :consumers      (doall (repeatedly consumer-parallelism spawn-consumer))
                 :ackers         (doall (repeatedly acker-parallelism spawn-acker))
                 :nackers        (doall (repeatedly nacker-parallelism spawn-nacker))})))]
    (let [state
          (atom (delay (launch)))

          shutdown-thread
          (Thread.
            ^Runnable
            (fn []
              (when (realized? (deref state))
                (log/debugf "Processor shutdown for %s initiated." queue-url)
                (let [{:keys [pipe
                              acker-chan
                              nacker-chan
                              producers
                              consumers
                              ackers
                              nackers
                              client]} (force (deref state))]
                  (log/debugf "Signaling producers and consumers to exit for %s processor." queue-url)
                  (async/close! pipe)
                  (run! async/<!! producers)
                  (log/debugf "Producers have exited for %s processor." queue-url)
                  (run! async/<!! consumers)
                  (log/debugf "Consumers have exited for %s processor." queue-url)
                  (log/debugf "Signaling ackers to exit for %s processor." queue-url)
                  (async/close! acker-chan)
                  (run! async/<!! ackers)
                  (log/debugf "Ackers have exited for %s processor." queue-url)
                  (log/debugf "Signaling nackers to exit for %s processor." queue-url)
                  (async/close! nacker-chan)
                  (run! async/<!! nackers)
                  (log/debugf "Nackers have exited for %s processor." queue-url)
                  (aws/stop client)
                  (log/debugf "Processor shutdown for %s finished." queue-url)))))

          system
          (reify PipedProcessor
            (start [this]
              (let [it (deref state)]
                (when-not (realized? it)
                  (.addShutdownHook (Runtime/getRuntime) shutdown-thread)
                  (force it)))
              this)
            (running? [this]
              (realized? (deref state)))
            (stop [this]
              (let [it (deref state)]
                (when (realized? it)
                  (.removeShutdownHook (Runtime/getRuntime) shutdown-thread)
                  (.run shutdown-thread)
                  (reset! state (delay (launch)))))
              this))]

      (swap! processors assoc queue-url system)

      system)))
