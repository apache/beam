Prompt:
Is PubSub supported in Apache Beam?
Response:
[PubSub](https://cloud.google.com/pubsub) is a[ Google Cloud](https://cloud.google.com/) service that provides a simple, reliable, scalable, and secure real-time messaging service for sending and receiving messages between independent applications. Apache Beam provides a PubSubIO connector that allows you to read and write messages from and to PubSub.
PubSub is currently supported only in streaming pipelines. 


PubSub is supported in the following Beam SDKs:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/pubsubio)
* [Typescript](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/pubsub.ts) via X-language

[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with PubSub and Apache Beam. See here for [read](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/pubsub/read_pubsub_multiple.py) and [write](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/pubsub/write_pubsub.py) examples in Python.

```python
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToPubSub

  with beam.Pipeline(options=options) as p:
    (p | "Read from PubSub" >> ReadFromPubSub(topic="input_topic")
       | "Write to PubSub" >> WriteToPubSub(topic="output_topic"))
```


