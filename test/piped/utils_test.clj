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


(deftest batching-test
  (let [received (atom [])
        msg1     {:data 1}
        msg2     {:data 2}
        msg3     {:data 3}
        chan     (async/chan)
        return   (batching chan 5000 5)]

    (async/go-loop []
      (when-some [item (async/<! return)]
        (swap! received conj item)
        (recur)))

    (async/>!! chan msg1)
    (async/>!! chan msg2)
    (async/>!! chan msg3)
    (is (empty? (deref received)))
    (async/close! chan)
    (async/<!! (async/timeout 100))
    (is (= 1 (count (deref received))))
    (is (= 3 (count (first (deref received)))))))
