<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
### Writing to Kafka using KafkaIO

When writing data processing pipelines using Apache Beam, developers can use the `WriteToKafka` transform to write data to Kafka topics. This transform takes a `PCollection` of data as input and writes the data to a specified Kafka topic using a Kafka producer.

To use the `WriteToKafka` transform, developers need to provide the following parameters:

* **producer_config**: a dictionary that contains the Kafka producer configuration properties, such as the Kafka broker addresses and the number of acknowledgments to wait for before considering a message as sent.
* **bootstrap.servers**: is a configuration property in Apache Kafka that specifies the list of bootstrap servers that the Kafka clients should use to connect to the Kafka cluster.
* **topic**: the name of the Kafka topic to write the data to.
* **key**: a function that takes an element from the input PCollection and returns the key to use for the Kafka message. The key is optional and can be None.
* **value**: a function that takes an element from the input PCollection and returns the value to use for the Kafka message.

When writing data to Kafka using Apache Beam, it is important to ensure that the pipeline is fault-tolerant and can handle failures, such as network errors, broker failures, or message serialization errors. Apache Beam provides features such as checkpointing, retries, and dead-letter queues to help developers build robust and reliable data processing pipelines that can handle these types of failures.

For detailed [information](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/io/kafka/KafkaIO.html)
{{if (eq .Sdk "go")}}
```
data := pubsubio.Read(s, "pubsub-public-data", "taxirides-realtime", nil)
kvData := beam.ParDo(s, func(elm []byte) ([]byte, []byte) { return []byte(""), elm }, data)
windowed := beam.WindowInto(s, window.NewFixedWindows(15*time.Second), kvData)
kafkaio.Write(s, *expansionAddr, *bootstrapServers, *topic, windowed)
```
{{end}}

{{if (eq .Sdk "java")}}
```
Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");
        producerProps.setProperty("key.serializer", StringSerializer.class.getName());
        producerProps.setProperty("value.serializer", ByteArraySerializer.class.getName());


        input.apply(KafkaIO.<String, String>write()
                .withBootstrapServers("localhost:9092")
                .withTopic("my-topic")
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withProducerConfigUpdates(new HashMap<>()).values());
```
{{end}}


{{if (eq .Sdk "python")}}
```
(input |  "Write to Kafka" >> WriteToKafka(
       topic=output_topic,
       producer_config = bootstrap_servers,
       key='key',
       value='value'))
```
{{end}}