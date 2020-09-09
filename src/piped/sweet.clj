(ns piped.sweet
  (:require [piped.core :as piped]
            [clojure.tools.logging :as log]
            [piped.utils :as utils]))


(defmacro defprocessor [symbol bindings attributes & body]
  `(when-not *compile-files*
     (let [attrs#  ~attributes
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
       var#)))
