[![Build Status](https://travis-ci.com/rutledgepaulv/piped.svg?branch=master)](https://travis-ci.com/rutledgepaulv/piped)
[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.rutledgepaulv/piped.svg)](https://clojars.org/org.clojars.rutledgepaulv/piped)

A Clojure library that enables applications to consume Amazon's Simple Queue Service (SQS) with minimal 
ceremony while achieving superior resilience and performance properties. Uses core.async behind the scenes 
to construct an efficient message processing system fed by SQS and provides easy high level Clojure APIs on top.

## Concepts

#### Pipe

A core.async channel that connects producers and consumers.

#### Producers

These poll SQS for messages and stuff them onto a channel. They automatically transition between
short and long polling based on the throughput and backpressure of your system.

#### Consumers

These read SQS messages from a channel and hand them to your message processing function. Consumers 
then supervise the message processing in order to extend visibility timeouts, nack failed messages, 
and ack messages that were processed successfully. Async/compute consumers run your processing function 
on one of the go-block dispatch threads and should only be used for cpu-bound tasks. Blocking consumers 
run your processing function on a dedicated thread and should be used for blocking-io. Blocking consumers
are the default when you create a system.

#### Message Processing Function

This is the code that you write. It receives a message and can do whatever it wants with it. 
If `:ack` or `:nack` are returned, the message will be acked or nacked. If an exception is thrown 
the message will both be nacked and count towards circuit breaking. Any other return values will be 
acked. When you have multiple kinds of messages in your queue a multimethod is a good choice.

#### System

A set of producers, consumers, and a pipe.


## Usage

```clojure 

(require '[piped.sweet :refer [defmultiprocessor])
(require '[piped.core :as piped])

; defines a multimethod and a core.async system 
; for polling and processing messages
(defmultiprocessor my-processor [{:keys [Body]}]
  {:queue-url            queue-url
   :consumer-parallelism 50
   ; there are more options supported here, like defining
   ; your own aws client if not using ambient credentials
  }
  (get Body :kind))

; normal clojure defmethod
(defmethod my-processor :alert [{{:keys [message]} :Body}]
  (Thread/sleep 500)
  (log/error message))

(defmethod my-processor :warn [{{:keys [message]} :Body}]
  (Thread/sleep 1000)
  (log/warn message))

; start the core.async system to process messages
(piped/start #'my-processor)

; stop the system. blocks until in-flight messages
; are done and nacks any messages that have been
; received but haven't started to be processed
(piped/stop #'my-processor)

```

## Features

#### Lightweight AWS
Uses [cognitect's AWS client](https://github.com/cognitect-labs/aws-api) for a more minimal library footprint.

#### Supports both Blocking IO and CPU Bound processors
Uses core.async for the internal machinery, but as a consumer you should be free to perform side effects, and you are.

#### Lease Extensions
If your consumer is still working on a message as it nears its visibility timeout, piped will extend the visibility timeout
for you instead of risking that another worker will start processing the same message.

#### Backpressure / SQS Rate Matching
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

