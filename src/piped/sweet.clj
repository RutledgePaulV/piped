(ns piped.sweet
  (:require [piped.core :as piped]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]))


(defmacro defmultiprocessor
  "Defines a clojure multimethod and constructs a new piped system from the attribute map.
   The multimethod is used as the consumer function for processing the queue. Multimethod
   var implements the piped.core/PipedSystem protocol so that the associated system can easily
   be started and stopped. For supported attributes see piped.core/processor.

   Redefining a processor for the same queue will stop the system for the old queue and start
   a new system using the most recent attributes. If the system was not running neither will
   the new system be running.

   Changing the queue-url for a processor will stop the system for the old queue-url and the
   new system for the new queue-url won't be started automatically.

   Unlike clojure.core/defmulti you may redefine the dispatch function without ceremony.
   "
  [symbol bindings attributes & body]
  `(let [attrs#                 ~attributes
         var#                   (utils/defmulti* ~symbol (fn ~bindings ~@body))
         queue-system#          (piped/get-system (:queue-url attrs#))
         queue-was-running#     (and queue-system# (piped/running? queue-system#))
         processor-system#      (some-> var# meta ::system)
         processor-was-running# (and processor-system# (piped/running? processor-system#))
         are-same-system#       (and queue-system# processor-system# (identical? queue-system# processor-system#))
         new-system#            (piped/processor (assoc attrs# :consumer-fn (deref var#)))]

     (defmethod ~symbol :default [msg#]
       (log/warnf "Received unfamiliar message %s. Message will be nacked." (get msg# :MessageId))
       :nack)

     (when queue-was-running#
       (log/infof "System for queue %s will be recreated because %s was reloaded." (:queue-url attrs#) (str var#))
       (future
         (piped/stop queue-system#)
         (log/infof "Previous system instance for queue %s has stopped." (:queue-url attrs#))))

     (when (and processor-was-running# (not are-same-system#))
       (log/infof "System for queue %s will be shutdown because the queue url for %s changed." (:queue-url attrs#) (str var#))
       (future
         (piped/stop processor-system#)
         (log/infof "Previously system instance for var %s has stopped." (str var#))))

     (when queue-was-running#
       (piped/start new-system#)
       (log/infof "New system for queue %s has been started." (:queue-url attrs#)))

     (let [metadata# {:piped.sweet/system new-system#
                      `piped/running?     (fn [& args#] (piped/running? new-system#))
                      `piped/start        (fn [& args#] (piped/start new-system#))
                      `piped/stop         (fn [& args#] (piped/stop new-system#))}]
       (alter-meta! var# merge metadata#))

     var#))

(defmacro defprocessor
  "Like defmultiprocessor, but defines a single message handling implementation instead
   of requiring defining a dynamic dispatch function and separate method implementation."
  [symbol bindings attributes & body]
  `(let [var# (defmultiprocessor ~symbol ~bindings ~attributes :default)]
     (defmethod ~symbol :default ~bindings ~@body)
     var#))