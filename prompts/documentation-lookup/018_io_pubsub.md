Prompt:
Is PubSub supported in Apache Beam?
Response:
[PubSub](https://cloud.google.com/pubsub) is a[ Google Cloud](https://cloud.google.com/) service that provides a simple, reliable, scalable, and secure real-time messaging service for sending and receiving messages between independent applications. Apache Beam provides a PubSubIO connector that allows you to read and write messages from and to PubSub topics.

PubSub is supported in the following Beam SDKs:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio)
* [Typescript](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/pubsub.ts) via X-language
  
