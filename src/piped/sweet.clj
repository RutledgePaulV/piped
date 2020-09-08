(ns piped.sweet
  (:require [piped.core :as piped]
            [clojure.tools.logging :as log]))


(defmacro defmulti*
  "Like clojure.core/defmulti, but actually updates the dispatch value when you reload it."
  [symbol dispatch-fn]
  `(let [dispatch-fun# ~dispatch-fn
         existing-var# (resolve '~symbol)]
     (if-some [dispatch# (some-> existing-var# meta ::holder)]
       (do (vreset! dispatch# dispatch-fun#) existing-var#)
       (let [holder# (volatile! dispatch-fun#)
             var#    (defmulti ~symbol (fn [& args#] (apply @holder# args#)))]
         (alter-meta! var# merge {::holder holder#})
         var#))))

(defmacro defprocessor [symbol bindings attributes & body]
  `(when-not *compile-files*
     (let [attrs#  ~attributes
           var#    (defmulti* ~symbol (fn ~bindings ~@body))
           system# (if-some [existing# (some-> var# meta ::system)]
                     (do (piped/stop existing#) (piped/create-system (assoc attrs# :consumer-fn (deref var#))))
                     (piped/create-system (assoc attrs# :consumer-fn var#)))]
       (defmethod ~symbol :default [msg#]
         (log/warnf "Received unfamiliar message %s. Message will be nacked." (get msg# :MessageId))
         :nack)
       (let [metadata# {::system     system#
                        `piped/start (fn [& args#] (piped/start system#))
                        `piped/stop  (fn [& args#] (piped/stop system#))}]
         (alter-meta! var# merge metadata#))
       var#)))
