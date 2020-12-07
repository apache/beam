<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam Template to ingest data from Apache Kafka to Google Cloud Pub/Sub

This directory contains an [Apache Beam](https://beam.apache.org/) pipeline example that creates a pipeline
to read data from a single or multiple topics from
[Apache Kafka](https://kafka.apache.org/) and write data into a single topic
in [Google Cloud Pub/Sub](https://cloud.google.com/pubsub).

Supported data formats:
- Serializable plaintext formats, such as JSON
- [PubSubMessage](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage).

Supported input source configurations:
- Single or multiple Apache Kafka bootstrap servers
- Apache Kafka SASL/SCRAM authentication over plaintext or SSL connection
- Secrets vault service [HashiCorp Vault](https://www.vaultproject.io/).

Supported destination configuration:
- Single Google Cloud Pub/Sub topic.

In a simple scenario, the template will create an Apache Beam pipeline that will read messages from a source Kafka server with a source topic, and stream the text messages into specified Pub/Sub destination topic. Other scenarios may need Kafka SASL/SCRAM authentication, that can be performed over plain text or SSL encrypted connection. The template supports using a single Kafka user account to authenticate in the provided source Kafka servers and topics. To support SASL authenticaton over SSL the template will need an SSL certificate location and access to a secrets vault service with Kafka username and password, currently supporting HashiCorp Vault.

## Requirements

- Java 8
- Kafka Bootstrap Server(s) up and running
- Existing source Kafka topic(s)
- An existing Pub/Sub destination output topic
- (Optional) An existing HashiCorp Vault
- (Optional) A configured secure SSL connection for Kafka

## Getting Started

This section describes what is needed to get the template up and running.
- Assembling the Uber-JAR
- Local execution
- Google Dataflow Template
  - Set up the environment
  - Creating the Dataflow Flex Template
  - Create a Dataflow job to ingest data using the template
- Avro format transferring.
- E2E tests (TBD)

## Assembling the Uber-JAR

To run this template the template Java project should be built into
an Uber JAR file.

Navigate to the Beam folder:

```
cd /path/to/beam
```

In order to create Uber JAR with Gradle, [Shadow plugin](https://github.com/johnrengelman/shadow)
is used. It creates the `shadowJar` task that builds the Uber JAR:

```
./gradlew -p examples/kafka-to-pubsub clean shadowJar
```

ℹ️ An **Uber JAR** - also known as **fat JAR** - is a single JAR file that contains
both target package *and* all its dependencies.

The result of the `shadowJar` task execution is a `.jar` file that is generated
under the `build/libs/` folder in kafka-to-pubsub directory.

## Local execution
To execute this pipeline locally, specify the parameters:
- Kafka Bootstrap servers
- Kafka input topics
- Pub/Sub output topic
- Output format

in the following format:
```bash
--bootstrapServers=host:port \
--inputTopics=your-input-topic \
--outputTopic=projects/your-project-id/topics/your-topic-pame \
--outputFormat=AVRO|PUBSUB
```
Optionally, to retrieve Kafka credentials for SASL/SCRAM,
specify a URL to the credentials in HashiCorp Vault and the vault access token:
```bash
--secretStoreUrl=http(s)://host:port/path/to/credentials
--vaultToken=your-token
```
Optionally, to configure secure SSL connection between the Beam pipeline and Kafka,
specify the parameters:
- A path to a truststore file (it can be a local path or a GCS path, which should start with `gs://`)
- A path to a keystore file (it can be a local path or a GCS path, which should start with `gs://`)
- Truststore password
- Keystore password
- Key password
```bash
--truststorePath=path/to/kafka.truststore.jks
--keystorePath=path/to/kafka.keystore.jks
--truststorePassword=your-truststore-password
--keystorePassword=your-keystore-password
--keyPassword=your-key-password
```
To change the runner, specify:
```bash
--runner=YOUR_SELECTED_RUNNER
```
See examples/java/README.md for steps and examples to configure different runners.

## Google Dataflow Execution

This example also exists as Google Dataflow Template, see its [README.md](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/v2/kafka-to-pubsub/README.md) for more information.

## AVRO format transferring.

This template contains an example demonstrating AVRO format support:
- Define custom Class to deserialize AVRO from Kafka [provided in example]
- Create custom data serialization in Apache Beam
- Serialize data to AVRO in Pub/Sub [provided in example].

To use this example in the specific case, follow these steps:

- Create your own class to describe AVRO schema. As an example use [AvroDataClass](src/main/java/org/apache/beam/templates/avro/AvroDataClass.java). Just define necessary fields.
- Create your own Avro Deserializer class. As an example use [AvroDataClassKafkaAvroDeserializer class](src/main/java/org/apache/beam/templates/avro/AvroDataClassKafkaAvroDeserializer.java). Just rename it, and put your own Schema class as the necessary types.
- Modify the [FormatTransform.readAvrosFromKafka method](src/main/java/org/apache/beam/templates/transforms/FormatTransform.java). Put your Schema class and Deserializer to the related parameter.
```java
return KafkaIO.<String, AvroDataClass>read()
        ...
        .withValueDeserializerAndCoder(
            AvroDataClassKafkaAvroDeserializer.class, AvroCoder.of(AvroDataClass.class)) // put your classes here
        ...
```
- [OPTIONAL TO IMPLEMENT] Add [Beam Transform](https://beam.apache.org/documentation/programming-guide/#transforms) if it necessary in your case.
- Modify the write step in the [KafkaToPubsub class](src/main/java/org/apache/beam/templates/KafkaToPubsub.java) by putting your Schema class to "writeAvrosToPubSub" step.
    - NOTE: if it changed during the transform, you should use changed one class definition.
```java
if (options.getOutputFormat() == FormatTransform.FORMAT.AVRO) {
      ...
          .apply("writeAvrosToPubSub", PubsubIO.writeAvros(AvroDataClass.class)); // put your SCHEMA class here

    }
```

## End to end tests
TBD

_Note: The Kafka to Pub/Sub job executed with a distributed runner supports SSL configuration with the certificate located only in GCS._
