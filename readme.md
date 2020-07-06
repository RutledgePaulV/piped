[![Build Status](https://travis-ci.com/rutledgepaulv/piped.svg?branch=master)](https://travis-ci.com/rutledgepaulv/piped)
[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.rutledgepaulv/piped.svg)](https://clojars.org/org.clojars.rutledgepaulv/piped)

A Clojure library for building applications that leverage Amazon's SQS by adapting between AWS and core.async primitives. 
AWS only provides a polling and bursty API but core.async is best as a continuous stream. Bridging from one to another in 
a way that achieves superior resilience and performance properties with minimal configuration is the chief goal 
of **piped**.

## Concepts

#### Pipe

A core.async channel that connects producers and consumers.

#### Producers

These poll SQS for messages and stuff them onto a channel. They automatically transition between
short and long polling based on the throughput and backpressure of your system.

#### Consumers

These read SQS messages from a channel and hand them to your message processing function. Consumers 
then supervise the message processing in order to extend visibility timeouts, trip circuits, 
nack failed messages, and ack messages that were processed successfully. Compute consumers 
run your processing function on one of the go-block dispatch threads and should only be used for 
cpu-bound tasks. Blocking consumers run your processing function on a dedicated thread and should 
be used for blocking-io. Blocking consumers are the default when you create a system.

#### Message Processing Function

This is the code that you write. It receives a message and can do whatever it wants with it. 
If `:ack` or `:nack` are returned, the message will be acked or nacked. If an exception is thrown 
the message will both be nacked and count towards circuit breaking. If anything else is 
returned then the message will be acked. If you have multiple kinds of messages in your queue 
a multimethod is a good choice.

#### Middleware

Nothing is provided for this, but you may certainly add a transducer to your pipe to transform 
(parse) messages prior to processing them. Remember that filtering a message out means it won't
be acked (never reaches the consumers or processing function) and will just be re-enqueued by 
SQS once the visibility timeout expires.

#### System

A set of producers, consumers, and a pipe.


## Usage

```clojure 

(require '[cognitect.aws.client.api :as aws])
(require '[piped.core :as piped])
(require '[clojure.edn :as edn])

(defmulti processor (comp :type :body))

(defmethod processor :hello [{{:keys [recipient message]} :body}]
    (my.app.email/send-email recipient message))

(defmethod processor :default [msg] 
    (println "Received message I'm not prepared to handle!")
    :nack)

(defn parse-body [msg]
   (update msg :body edn/read-string))

(def middleware (map parse-body))

(def pipe (async/chan 100 middleware))

(def queue-url "https://queue.amazonaws.com/80398EXAMPLE/MyQueue")

(def client (aws/client {:api :sqs}))

(def stop-callback (piped/spawn-system client queue-url processor {:pipe pipe}))

; ... some time later

; stop the system!
; this will nack any messages that have been received but not yet put into the pipe
; and will wait for consumers to finish processing messages that are already in the pipe
(stop-callback)

```

## Features

#### Lightweight AWS
Uses [cognitect's AWS client](https://github.com/cognitect-labs/aws-api) for a more minimal library footprint.

#### Supports both Blocking IO and CPU Bound processors
Uses core.async for the internal machinery, but as a consumer you should be free to perform side effects, and you are.

#### Backpressure
If your consumer isn't keeping up, producers will read less frequently from SQS in order to match your consumption rate.

#### Circuit Breaking
If a consumer begins throwing on everything they process, piped exerts back pressure to read from SQS less often. No sense
in reading a bunch of messages just to nack them all.

#### Lease Extensions
If your consumer is still working on a message as it nears its visibility timeout, piped will extend the visibility timeout
for you instead of risking that another worker will start processing the same message.

#### SQS Rate Matching
When your consumer and SQS are both speeding along, producers will start polling SQS in a tighter loop. If SQS is 
barely producing messages, then producers will poll SQS in a longer loop to decrease your costs.

#### Efficient Batching
Messages are read and acked in batches when possible, but in a way that tries to present your application with a continuous
stream instead of erratic bursts.


## Alternatives

[Squeedo](https://github.com/TheClimateCorporation/squeedo)

Frankly I made this library because of perceived deficiencies in Squeedo and as such can't recommend it.
Squeedo inappropriately performs blocking-io from go blocks when receiving and acking messages which can
lead to poor performance. It is my opinion that Squeedo doesn't provide enough leverage over the raw AWS SDK. 
YMMV.

- [Performing a blocking ReceiveMessage call on a go thread](https://github.com/TheClimateCorporation/squeedo/blob/master/src/com/climate/squeedo/sqs_consumer.clj#L34-L36)
- [Performing blocking acks/nacks on a go thread](https://github.com/TheClimateCorporation/squeedo/blob/master/src/com/climate/squeedo/sqs_consumer.clj#L87-L91)

If you're unaware of the dangers of mixing blocking-io and go blocks, please read [this excellent post](https://eli.thegreenplace.net/2017/clojure-concurrency-and-blocking-with-coreasync/).

