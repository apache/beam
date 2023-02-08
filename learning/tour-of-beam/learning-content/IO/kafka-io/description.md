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
### KafkaIO

KafkaIO is a part of the Apache Beam SDK that provides a way to read data from Apache Kafka and write data to it. It allows for the creation of Beam pipelines that can consume data from a Kafka topic, process the data and write the processed data back to another Kafka topic. This makes it possible to build data processing pipelines using Apache Beam that can easily integrate with a Kafka-based data architecture.

{{if (eq .Sdk "go")}}
```
var (
	expansionAddr = flag.String("expansion_addr", "",
		"Address of Expansion Service. If not specified, attempts to automatically start an appropriate expansion service.")
	bootstrapServers = flag.String("bootstrap_servers", "",
		"(Required) URL of the bootstrap servers for the Kafka cluster. Should be accessible by the runner.")
	topic = flag.String("topic", "kafka_taxirides_realtime", "Kafka topic to write to and read from.")
)

read := kafkaio.Read(s, *expansionAddr, *bootstrapServers, []string{*topic})
```
{{end}}

{{if (eq .Sdk "java")}}
```
p.apply("ReadFromKafka",
                        KafkaIO.<String, String>read()
                                .withBootstrapServers("localhost:29092")
                                .withTopicPartitions(
                                        Collections.singletonList(
                                                new TopicPartition(
                                                        "NYCTaxi1000_simple",
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withConsumerConfigUpdates(consumerConfig)
                                .withMaxNumRecords(998)
                                .withoutMetadata())
```
{{end}}


{{if (eq .Sdk "python")}}
```
input_topic = 'input-topic'
output_topic = 'output-topic'

(p | "Read from Kafka" >> ReadFromKafka(
      topics=[input_topic],
      bootstrap_servers='localhost:9092')
 | "Process data" >> beam.Map(process_data)
 | "Write to Kafka" >> WriteToKafka(
      topic=output_topic,
      bootstrap_servers='localhost:9092',
      key='key',
      value='value')
)
```
{{end}}