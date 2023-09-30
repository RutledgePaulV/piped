(ns piped.utils-test
  (:require [piped.utils :refer :all]
            [clojure.core.async :as async]
            [clojure.test :refer :all]))

(deftest deadline-batching-test
  (let [received (atom [])
        msg1     (with-meta {:data 1} {:deadline (async/timeout 3500)})
        msg2     (with-meta {:data 2} {:deadline (async/timeout 4000)})
        msg3     (with-meta {:data 3} {:deadline (async/timeout 5000)})
        chan     (async/chan)
        return   (deadline-batching chan 5)]

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

(deftest interval-batching-test
  (let [received (atom [])
        msg1     {:data 1}
        msg2     {:data 2}
        msg3     {:data 3}
        chan     (async/chan)
        return   (interval-batching chan 5000 5)]

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

(deftest combo-batching-test
  (let [received (atom ())
        msg1     {:data 1}
        msg2     {:data 2}
        msg3     (-> {:data 3}
                     (with-deadline 300)
                     (with-timeout 10000))
        chan     (async/chan)
        return   (combo-batching chan 1001 5)]

    (async/>!! chan msg1)
    (async/>!! chan msg2)
    (is (empty? (deref received)))
    (async/<!! (async/timeout 100))
    (nil? (async/poll! return))
    (async/>!! chan msg3)
    (let [start (System/currentTimeMillis)
          batch (async/<!! return)
          stop  (System/currentTimeMillis)]
      (is (= 3 (count batch)))
      (is (< (- stop start) 600)))))
