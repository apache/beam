Prompt:
Is Apache Kafka supported in Apache Beam?

Response:
[Apache Kafka](https://kafka.apache.org/) is a distributed streaming platform that is used for building real-time data pipelines and streaming applications. It is horizontally scalable, fault-tolerant, and fast.
Apache Beam has a built-in KafkaIO connector for Apache Kafka that lets you read data from and write data to Kafka. Apache Kafka is supported in both batch pipelines and streaming pipelines, and as a source and a sink. For more information, see the KafkaIO connector documentation:
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.html)
* [Python (using cross-language transforms)](https://beam.apache.org/releases/pydoc/current/apache_beam.io.kafka.html)
* [Go (using cross-language transforms)](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio)
* [Typescript (using cross-language transforms)](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/kafka.ts)

The [Dataflow cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) can help you to get started with Apache Kafka and Apache Beam.

Apache Beam pipeline code for reading data from a Kafka topic might look like the following example:

```python
with beam.Pipeline(options=options) as p:
    output = (p | "Read from Kafka" >> ReadFromKafka(
                        consumer_config={'bootstrap.servers': 'localhost:9092'},
                        topics=['my_topic'],
                        with_metadata=False)
                        | "Log Data" >> Map(logging.info))
```
This code reads data from a Kafka topic `my_topic` on `localhost:9092` and logs the data to the console.

The Apache Beam repository has a [KafkaIO example](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/kafkataxi/kafka_taxi.py) that demonstrates how to use the KafkaIO connector.

