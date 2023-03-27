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

`KafkaIO` is a part of the Apache Beam SDK that provides a way to read data from Apache Kafka and write data to it. It allows for the creation of Beam pipelines that can consume data from a Kafka topic, process the data and write the processed data back to another Kafka topic. This makes it possible to build data processing pipelines using Apache Beam that can easily integrate with a Kafka-based data architecture.

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