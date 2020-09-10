(defproject org.clojars.rutledgepaulv/piped "0.1.0"

  :description
  "A library for processing AWS SQS messages from a Clojure application."

  :url
  "https://github.com/rutledgepaulv/piped"

  :license
  {:name "MIT License" :url "http://opensource.org/licenses/MIT" :year 2020 :key "mit"}

  :scm
  {:name "git" :url "https://github.com/rutledgepaulv/piped"}

  :pom-addition
  [:developers
   [:developer
    [:name "Paul Rutledge"]
    [:url "https://github.com/rutledgepaulv"]
    [:email "rutledgepaulv@gmail.com"]
    [:timezone "-5"]]]

  :deploy-repositories
  [["releases" :clojars]
   ["snapshots" :clojars]]

  :dependencies
  [[org.clojure/clojure "1.10.1"]
   [com.cognitect.aws/api "0.8.456"]
   [com.cognitect.aws/endpoints "1.1.11.813"]
   [com.cognitect.aws/sqs "801.2.702.0"]
   [org.clojure/core.async "1.2.603"]
   [org.clojure/tools.logging "1.1.0"]]

  :plugins
  [[lein-cloverage "1.1.2"]]

  :profiles
  {:test
   {:dependencies
    [[org.testcontainers/testcontainers "1.14.3" :scope "test"]
     [org.slf4j/slf4j-simple "1.7.30" :scope "test"]]}}

  :repl-options
  {:init-ns piped.core})
