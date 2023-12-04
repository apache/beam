Prompt:
Is Google BigTable supported in Apache Beam?
Response:
BigTable is a Google Cloud service that provides a NoSQL database. Apache Beam provides a BigTableIO connector that allows you to read and write data from and to BigTable.












-----------------------------------------------
[PubSub](https://cloud.google.com/pubsub) is a[ Google Cloud](https://cloud.google.com/) service that provides a simple, reliable, scalable, and secure real-time messaging service for sending and receiving messages between independent applications. Apache Beam provides a PubSubIO connector that allows you to read and write messages from and to Pub/Sub.
Pub/Sub is currently supported only in streaming pipelines.


Pub/Sub is supported in the following Beam SDKs:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigtable/BigtableIO.html)
* [Python](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html)
* [Via xlang](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.bigtableio.html)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigtableio)
* [Go](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigtableio)

[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with Pub/Sub and Apache Beam. See here for [read](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/pubsub/read_pubsub_multiple.py) and [write](https://github.com/GoogleCloudPlatform/dataflow-cookbook/blob/main/Python/pubsub/write_pubsub.py) examples in Python.

```python
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToPubSub

  with beam.Pipeline(options=options) as p:
    (p | "Read from Pub/Sub" >> ReadFromPubSub(topic="input_topic")
       | "Write to Pub/Sub" >> WriteToPubSub(topic="output_topic"))
```




https://beam.apache.org/performance/bigtable
