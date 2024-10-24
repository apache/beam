Prompt:
Is Pub/Sub supported in Apache Beam?

Response:
[Pub/Sub](https://cloud.google.com/pubsub) is a [Google Cloud](https://cloud.google.com/) service that provides a simple, reliable, scalable, and secure real-time messaging service for sending and receiving messages between independent applications. Apache Beam provides a PubSubIO connector that allows you to read and write messages from and to Pub/Sub. Pub/Sub is currently supported only in streaming pipelines.

Pub/Sub is supported in the following Beam SDKs:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio)
* [Typescript](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/pubsub.ts) (through X Language)

To get started with Pub/Sub and Apache Beam, refer to the [Dataflow Cookbook GitHub repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook). The repository provides Python code examples that [read](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/pubsub/read_pubsub_multiple.py) and [write](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/pubsub/write_pubsub.py) data from and to Pub/Sub.

Here is an example of the Apache Beam pipeline code for reading data from a Pub/Sub topic and writing it to another topic:

```python
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToPubSub

  with beam.Pipeline(options=options) as p:
    (p | "Read from Pub/Sub" >> ReadFromPubSub(topic="input_topic")
       | "Write to Pub/Sub" >> WriteToPubSub(topic="output_topic"))
```
