```clojure 

; process is humming along

"Beginning new long poll of sqs queue https://sqs.us-east-1.amazonaws.com/933109431395/piped."
"All messages polled from https://sqs.us-east-1.amazonaws.com/933109431395/piped were accepted by consumers."
"Beginning new long poll of sqs queue https://sqs.us-east-1.amazonaws.com/933109431395/piped."
"Processed message 03f0bb54-4438-4072-902f-63299d177bf2"
"Processed message c326956b-223a-4c4d-8e30-10182d02c2f0"
"Processed message f7c19e2e-2040-4444-8c72-61182ce4309d"
"Processed message cfab984a-3491-48a0-a4d9-904ca1258976"
"Processed message 986e9edf-373c-4e50-9b88-97f73e08ccbf"
"Processed message 37f8b8b2-bc7c-4b58-9d9f-3532ad5e850d"
"Processed message cb0b4e46-d361-4144-bc40-8825b8087286"
"Processed message f73e8713-cd11-4176-a348-dc8404afbec5"
"Processed message 5aa7d469-40f2-4a31-9882-80f601b00ae4"
"Processed message b40df239-29df-436c-a4db-fb98fbafbef7"
"All messages polled from https://sqs.us-east-1.amazonaws.com/933109431395/piped were accepted by consumers."
"Beginning new long poll of sqs queue https://sqs.us-east-1.amazonaws.com/933109431395/piped."

; process was signaled with SIGTERM

"Processor shutdown for https://sqs.us-east-1.amazonaws.com/933109431395/piped initiated."
"Signaling producers and consumers to exit for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."

; finishing processing in-flight messages

"Processed message 15fdb8a9-8f6f-4ca2-a9dd-d834c6443e48"
"Processed message d65a29ee-0333-4ebc-a5f3-a14f84c6e363"
"Processed message 3b71aa83-960d-4e36-ae08-a5e24b1c009a"
"Processed message 205543fd-b9a4-4207-a104-39d6db618bec"
"Processed message 9cf5db50-18bc-41b4-bfa6-807fc919f074"
"Processed message e4ca5693-693d-4c35-8445-ee4efff2dd4e"
"Processed message 42603834-5f4f-457c-848a-252cb836343c"
"Processed message 471f5e46-b4e4-49b5-98c9-992826ca8665"
"Processed message e8569316-becf-487d-a5ef-2cc4388b0de1"
"Processed message cb2ea600-0004-40b5-9f78-9f8b83e99f42"

; has now completely stopped receiving messages from SQS 
"Producer stopping because channel for queue https://sqs.us-east-1.amazonaws.com/933109431395/piped has been closed."
"Producers have exited for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."

; last consumer finishes last in-flight message
"Processed message ad6d4d3b-40b7-4436-a90d-e3f927d6926c"
"Consumers have exited for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."

; ackers are closed now that they have their final messages to ack
"Signaling ackers to exit for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."
"Ackers have exited for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."

; nackers are closed now that they have their final messages to nack
"Signaling nackers to exit for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."
"Nackers have exited for https://sqs.us-east-1.amazonaws.com/933109431395/piped processor."

; processor has finished shutting down gracefully
"Processor shutdown for https://sqs.us-east-1.amazonaws.com/933109431395/piped finished."

; process exited

```