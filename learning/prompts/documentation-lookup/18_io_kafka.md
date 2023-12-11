Prompt:
Is Apache Kafka supported in Apache Beam?
Response:
[Apache Kafka](https://kafka.apache.org/) is a distributed streaming platform that is used for building real-time data pipelines and streaming applications. It is horizontally scalable, fault-tolerant, and fast.
Apache Beam has a `KafkaIO` built-in connector for Apache Kafka that allows you to read and write data from and to Kafka. Apache kafka is supported both in batch and streaming pipelines and as a source and a sink.
* [Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/kafka/KafkaIO.html)
* [Python via X-language](https://beam.apache.org/releases/pydoc/current/apache_beam.io.kafka.html)
* [Go via X-language](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/kafkaio)
* [Typescript  via X-language](https://github.com/apache/beam/blob/master/sdks/typescript/src/apache_beam/io/kafka.ts)

For performance metrics of `KafkaIO` connector, see [KafkaIO Performance](https://beam.apache.org/performance/io/kafka/).

[Dataflow-cookbook repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) will help you to get started with Apache Kafka and Apache Beam.

Apache Beam pipeline code for reading data from Kafka topic might look like the following example:

```python
with beam.Pipeline(options=options) as p:
    output = (p | "Read from Kafka" >> ReadFromKafka(
                        consumer_config={'bootstrap.servers': 'localhost:9092'},
                        topics=['my_topic'],
                        with_metadata=False)
                        | "Log Data" >> Map(logging.info))
```
This code reads data from a Kafka topic `my_topic` on `localhost:9092` and logs the data to the console.

Apache Beam repository has a [KafkaIO example](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/kafkataxi/kafka_taxi.py)  that demonstrates how to use KafkaIO connector.

