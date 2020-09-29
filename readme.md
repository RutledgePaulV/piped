[![Build Status](https://travis-ci.com/rutledgepaulv/piped.svg?branch=master)](https://travis-ci.com/rutledgepaulv/piped)
[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.rutledgepaulv/piped.svg)](https://clojars.org/org.clojars.rutledgepaulv/piped)

<img src="./docs/ready-hans.jpg" title="harmonic labyrinth" width="300" height="300" align="left" padding="5px"/>
<small>
<br/><br/><br/><br/>
A Clojure library that enables applications to consume messages from Amazon's Simple Queue Service (SQS) 
with minimal ceremony while achieving superior resilience and performance properties. Uses core.async to 
construct an efficient processing machine behind the scenes and provides an easy high level API.
</small>
<br clear="all" /><br />

---

### Features

#### Lightweight Async AWS
Uses [cognitect's AWS client](https://github.com/cognitect-labs/aws-api) for a minimal library footprint and true 
non-blocking io via an asynchronous jetty client.

#### Supports both Blocking IO and CPU Bound processors
Uses core.async for the internal machinery, but as a consumer you should be free to perform blocking io if you need to
and you are. Blocking consumers are the default mode but you can opt into non-blocking consumers if that's your style.

#### Lease Extensions
If your consumer is still working on a message as it nears its visibility timeout, piped will extend the visibility timeout
for you instead of risking that another worker will start processing the same message. 

#### Backpressure / SQS Rate Matching
Piped implements backpressure between the processing of a message and the receiving of messages from SQS so that you're
never grabbing items off the queue that you aren't ready to process in a timely manner.

#### Efficient Batching
Messages are read and acked in batches which decreases your costs and improves throughput. Ack batches are accumulated until
the max batch size is met or one of the messages in the batch is near expiry and therefore needs to be acked promptly rather
than waiting for the batch to finish filling up.

#### Minimal Configuration
Most users should only need to provide two things: the `queue-url` and the `consumer-parallelism` for message processing. Piped
automatically chooses the number of sqs polling processes needed to saturate your consumers.

---


### Normal Function Usage

```clojure 
(require '[piped.core :as piped])

(def opts
  {; *required* 
   ; the full url to the sqs queue
   :queue-url            "http://localhost:4576/queue/piped-test-queue17184"

   ; *required* 
   ; the function that gets called to process each message
   :consumer-fn          (fn [msg] (println msg))

   ; *optional* - defaults to 10
   ; maximum number of messages that should be processed concurrently
   ; you should tune this according to your available resources and 
   ; desired throughput. 
   :consumer-parallelism 10

   ; *optional* - defaults to identity
   ; a pure function to preprocess the message before it arrives at 
   ; your consumer-fn. intended for parsing the message body into data
   :transform-fn         identity

   ; *optional* - defaults to {}
   ; configuration passed to the aws-api client in case you need to customize 
   ; things like the credentials provider or endpoints for testing against
   :client-opts          {}

   ; *optional* - defaults to true
   ; whether to create dedicated threads for processing each message
   ; or if the processing should share the core.async dispatch thread
   ; pool (only set this to false if you're doing non-blocking io)
   :blocking-consumers   true})

; registers a processor with the above config
(def processor (piped/processor opts))

; start polling and processing messages
(piped/start processor)

; stop the system. blocks until in-flight messages
; are done and nacks any messages that have been
; received but haven't started to be processed
(piped/stop processor)

```

### Syntactic Sugar

```clojure 

(require '[clojure.edn :as edn])
(require '[piped.sweet :refer [defmultiprocessor]])
(require '[piped.core :as piped])

; defines a clojure multimethod and a piped processor system attached to the var
; the map of options is the same as seen in the earlier example but you may omit
; the consumer-fn (the multimethod satisfies that).

(defmultiprocessor my-processor [{:keys [Body]}]
  {:queue-url            "http://localhost:4576/queue/piped-test-queue17184"
   :consumer-parallelism 50
   :transform-fn         #(update % :Body edn/read-string)}
  (get Body :kind))

; define normal clojure defmethods to process message variants
; there is already a :default clause defined to log and nack any
; message for which there is no matching defmethod

(defmethod my-processor :alert [message]
  (log/error (get-in message [:Body :message])))

(defmethod my-processor :warn [message]
  (log/warn (get-in message [:Body :message])))

; start the processor by invoking protocol function on the var
(piped/start #'my-processor)

; stop the processor by invoking protocol function on the var
(piped/stop #'my-processor)

```

---

### Performance

Early testing shows around 6500 messages/second of throughput (received, printed, and acked) 
when using a `consumer-parallelism` of 1000. This exceeds the AWS throughput cap of 3000 for 
fifo queues with just one process. Obviously the throughput will decrease once you're doing 
more with each message than printing it. Benchmarking suite forthcoming.

---


### Internal Concepts

#### Pipe

A core.async channel that connects producers and consumers.

#### Producers

These long poll SQS for messages and stuff them onto the pipe. If the pipe doesn't accept their
messages in a timely manner then they start asking SQS for fewer messages at a time to match the
consumption rate.

#### Consumers

These read SQS messages from the pipe and hand them to your message processing function. Consumers 
supervise the message processing in order to extend visibility timeouts, nack failed messages, 
and ack messages that were processed successfully. 

Async/compute consumers run your processing function on one of the go-block dispatch threads and 
should only be used for cpu-bound tasks. Blocking consumers run your processing function on a 
dedicated thread and should be used for blocking-io. Blocking consumers are the default when 
you create a system.

#### Ackers

These collect messages into batches and ack them. They'll ack a batch as soon as the batch is full or
one of the messages in the pending batch is about to exceed its visibility timeout.

#### Nackers

These collect messages into batches and nack them. They'll nack a batch as soon as the batch is full
or the batch has been accumulating for longer than 5 seconds.

#### Consumer function

This is the code that you write. It receives a message and can do whatever it wants with it. 
If `:ack` or `:nack` are returned, the message will be acked or nacked. If an exception is thrown 
the message will be nacked. Any other return values will be acked. When you have multiple kinds of
messages in your queue a multimethod is a good choice.

#### Processor

A set of the above abstractions that can be started and stopped.

---

### Alternatives

[Squeedo](https://github.com/TheClimateCorporation/squeedo)

Frankly I made this library because of perceived deficiencies in Squeedo and as such can't recommend it.
Squeedo inappropriately performs blocking-io from go blocks when receiving and acking messages which can
lead to poor performance. It is my opinion that Squeedo also doesn't provide enough leverage over the raw AWS SDK. 
YMMV.

- [Performing a blocking ReceiveMessage call on a go thread](https://github.com/TheClimateCorporation/squeedo/blob/master/src/com/climate/squeedo/sqs_consumer.clj#L34-L36)
- [Performing blocking acks/nacks on a go thread](https://github.com/TheClimateCorporation/squeedo/blob/master/src/com/climate/squeedo/sqs_consumer.clj#L87-L91)

If you're unaware of the dangers of mixing blocking-io and go blocks, please read [this excellent post](https://eli.thegreenplace.net/2017/clojure-concurrency-and-blocking-with-coreasync/).

---

### Acknowledgements

- [Konrad Tallman](https://github.com/komcrad) for teaching me about SQS these last months and helping kick the tires.

---

### License

This project is licensed under [MIT license](http://opensource.org/licenses/MIT).
