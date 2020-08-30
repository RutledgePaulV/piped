(ns piped.utils-test
  (:require [clojure.test :refer :all])
  (:require [piped.utils :refer :all]
            [clojure.core.async :as async]))


(defn deadline-chan [millis]
  (async/go (async/<! (async/timeout millis)) :timeout))

(deftest deadline-batching-test
  (let [received (atom [])
        msg1     {:data 1 :deadline (deadline-chan 3500)}
        msg2     {:data 2 :deadline (deadline-chan 4000)}
        msg3     {:data 3 :deadline (deadline-chan 5000)}
        chan     (async/chan)
        return   (deadline-batching chan 5 :deadline)]

    (async/go-loop []
      (when-some [batch (async/<! return)]
        (swap! received conj batch)
        (recur)))

    (async/>!! chan msg1)
    (async/>!! chan msg2)
    (async/>!! chan msg3)
    (is (empty? (deref received)))
    (async/<!! (async/timeout 3600))
    (is (= 1 (count (deref received))))
    (is (= 3 (count (first (deref received)))))))

