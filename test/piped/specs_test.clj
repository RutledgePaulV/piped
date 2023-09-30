(ns piped.specs-test
  (:require
    [clojure.spec.alpha :as s]
    [clojure.test :refer :all]))

(deftest specs-test
  (testing "processor required fields"
    (let [spec {:queue-url   "http://queue-url"
                :consumer-fn (fn [msg] (constantly msg) :ack)}]
      (is (s/valid? :piped/options-map spec))
      (is (not (s/valid? :piped/options-map (spec dissoc :queue-url))))
      (is (not (s/valid? :piped/options-map (spec dissoc :consumer-fn))))))
  (testing "action maps"
    (let [spec {:action :ack :delay-seconds 5}]
      (is (s/valid? :piped/action-map spec))
      (is (s/valid? :piped/action-map (assoc spec :action :nack)))
      (is (s/valid? :piped/action-map (dissoc spec :delay-seconds)))
      (is (not (s/valid? :piped/action-map (assoc spec :delay-seconds "a"))))
      (is (not (s/valid? :piped/action-map (assoc spec :delay-seconds -1))))
      (is (not (s/valid? :piped/action-map (assoc spec :delay-seconds 1.13))))
      (is (not (s/valid? :piped/action-map (assoc spec :action :do-something)))))))
