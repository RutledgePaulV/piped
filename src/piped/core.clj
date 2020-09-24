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

(defprotocol PipedSystem
  :extend-via-metadata true
  (start [this] "Start polling SQS and processing messages.")
  (stop [this] "Stop the system and await completion of in-flight messages.")
  (running? [this] "Is the system currently running?"))

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

(defn get-system
  "Gets the system for a given queue url. Returns nil if there is no such system."
  [queue-url]
  (get @systems queue-url))

(defn http-client
  "Returns a http client using cognitect's async jetty client wrapper."
  [http-opts]
  (let [c (impl/create http-opts)]
    (reify http/HttpClient
      (-submit [_ request channel]
        (impl/submit c request channel))
      (-stop [_]
        (impl/stop c)))))

(defn create-system
  "Spawns a set of producers and consumers for a given queue.

   Returns a function of no arguments that can be called to stop the system."
  [{:keys [client-opts
           queue-url
           consumer-fn
           producer-parallelism
           consumer-parallelism
           acker-parallelism
           nacker-parallelism
           blocking-consumers
           transform]
    :as   opts}]

  (specs/assert-options opts)

  (letfn [(launch []
            (let [consumer-parallelism (or consumer-parallelism 10)
                  blocking-consumers   (if (boolean? blocking-consumers) blocking-consumers true)
                  producer-parallelism (or producer-parallelism (utils/quot+ consumer-parallelism 10))
                  acker-parallelism    (or acker-parallelism producer-parallelism)
                  nacker-parallelism   (or nacker-parallelism producer-parallelism)
                  max-http-ops         (+ producer-parallelism acker-parallelism nacker-parallelism)
                  client               (cond-> (or client-opts {})
                                         (not (contains? client-opts :http-client))
                                         (assoc :http-client
                                                (http-client
                                                  {:pending-ops-limit               max-http-ops
                                                   :max-connections-per-destination max-http-ops}))
                                         :always
                                         (assoc :api :sqs)
                                         :always
                                         (aws/client))
                  transform            (if transform
                                         (fn [msg]
                                           (try
                                             (transform msg)
                                             (catch Exception e
                                               (log/error e "Error in transformer.")
                                               msg)))
                                         identity)
                  acker-chan           (async/chan)
                  nacker-chan          (async/chan)
                  pipe                 (async/chan)
                  transformed          (async/map transform [pipe])
                  acker-batched        (utils/deadline-batching acker-chan 10 utils/message->deadline)
                  nacker-batched       (utils/interval-batching nacker-chan 5000 10)]

              (letfn [(spawn-producer []
                        (let [opts {:MaxNumberOfMessages (min 10 consumer-parallelism)}]
                          (producers/spawn-producer client queue-url pipe nacker-chan opts)))

                      (spawn-consumer []
                        (if blocking-consumers
                          (consumers/spawn-consumer-blocking client transformed acker-chan nacker-chan consumer-fn)
                          (consumers/spawn-consumer-async client transformed acker-chan nacker-chan consumer-fn)))

                      (spawn-acker []
                        (actions/spawn-acker client acker-batched))

                      (spawn-nacker []
                        (actions/spawn-nacker client nacker-batched))]

                {:client         client
                 :transform      transform
                 :acker-chan     acker-chan
                 :nacker-chan    nacker-chan
                 :pipe           pipe
                 :transformed    transformed
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
                (let [{:keys [pipe
                              acker-chan
                              nacker-chan
                              producers
                              consumers
                              ackers
                              nackers
                              client]} (force (deref state))]
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
                  (run! async/<!! nackers)
                  ; close any http resources
                  (aws/stop client)))))

          system
          (reify PipedSystem
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

      (swap! systems assoc queue-url system)

      system)))




