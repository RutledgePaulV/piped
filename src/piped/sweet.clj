(ns piped.sweet
  (:require [piped.core :as piped]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]))


(defmacro defmultiprocessor
  "Defines a clojure multimethod and constructs a new piped system from the attribute map.
   The multimethod is used as the consumer function for processing the queue. Multimethod
   var implements the piped.core/PipedSystem protocol so that the associated system can easily
   be started and stopped. For supported attributes see piped.core/create-system.

   Redefining a processor for the same queue will stop the system for the old queue and start
   a new system using the most recent attributes. If the system was not running neither will
   the new system be running.

   Changing the queue-url for a processor will stop the system for the old queue-url and the
   new system for the new queue-url won't be started automatically.

   Unlike clojure.core/defmulti you may redefine the dispatch function without ceremony.
   "
  [symbol bindings attributes & body]
  `(let [attrs#  ~attributes
         var#    (utils/defmulti* ~symbol (fn ~bindings ~@body))
         system# (let [queue-system#          (piped/get-system (:queue-url attrs#))
                       queue-was-running#     (and queue-system# (piped/running? queue-system#))
                       processor-system#      (some-> var# meta ::system)
                       processor-was-running# (and processor-system# (piped/running? processor-system#))
                       are-same-system#       (and queue-system# processor-system# (identical? queue-system# processor-system#))]
                   (when queue-was-running#
                     (log/infof "System for queue %s is being restarted because %s was reloaded." (:queue-url attrs#) (str var#))
                     (piped/stop queue-system#))
                   (when (and processor-was-running# (not are-same-system#))
                     (log/infof "System for queue %s will be shutdown because the queue url for %s changed." (:queue-url attrs#) (str var#))
                     (piped/stop processor-system#))
                   (cond-> (piped/create-system (assoc attrs# :consumer-fn var#))
                     queue-was-running# (piped/start)))]
     (defmethod ~symbol :default [msg#]
       (log/warnf "Received unfamiliar message %s. Message will be nacked." (get msg# :MessageId))
       :nack)
     (let [metadata# {::system        system#
                      `piped/running? (fn [& args#] (piped/running? system#))
                      `piped/start    (fn [& args#] (piped/start system#))
                      `piped/stop     (fn [& args#] (piped/stop system#))}]
       (alter-meta! var# merge metadata#))
     var#))

(defmacro defprocessor
  "Like defmultiprocessor, but defines a single message handling implementation instead
   of requiring defining a dynamic dispatch function and separate method implementation."
  [symbol bindings attributes & body]
  `(let [var# (defmultiprocessor ~symbol ~bindings ~attributes :default)]
     (defmethod ~symbol :default ~bindings ~@body)
     var#))