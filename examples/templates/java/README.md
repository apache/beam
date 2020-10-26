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

This directory contains an [Apache Beam](https://beam.apache.org/) Template that creates a pipeline
to read data from a single or multiple topics from
[Apache Kafka](https://kafka.apache.org/) and write data into a single topic
in [Google Pub/Sub](https://cloud.google.com/pubsub).

Supported data formats:
- Serializable plaintext formats, such as JSON
- [PubSubMessage](https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage).

Supported input source configurations:
- Single or multiple Apache Kafka bootstrap servers
- Apache Kafka SASL/SCRAM authentication over plaintext or SSL connection
- Secrets vault service [HashiCorp Vault](https://www.vaultproject.io/).

Supported destination configuration:
- Single Google Pub/Sub topic.

In a simple scenario, the template will create an Apache Beam pipeline that will read messages from a source Kafka server with a source topic, and stream the text messages into specified Pub/Sub destination topic. Other scenarios may need Kafka SASL/SCRAM authentication, that can be performed over plain text or SSL encrypted connection. The template supports using a single Kafka user account to authenticate in the provided source Kafka servers and topics. To support SASL authenticaton over SSL the template will need an SSL certificate location and access to a secrets vault service with Kafka username and password, currently supporting HashiCorp Vault.

## Requirements

- Java 11
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
  - Create a Dataflow job to ingest data using the template.
- Avro format transferring.

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
./gradlew -p examples/templates/java/kafka-to-pubsub clean shadowJar
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
in the following format:
```bash
--bootstrapServers=host:port \
--inputTopics=your-input-topic \
--outputTopic=projects/your-project-id/topics/your-topic-pame
```
Optionally, to retrieve Kafka credentials for SASL/SCRAM,
specify a URL to the credentials in HashiCorp Vault and the vault access token:
```bash
--secretStoreUrl=http(s)://host:port/path/to/credentials
--vaultToken=your-token
```
Optionally, to configure secure SSL connection between the Beam pipeline and Kafka,
specify the parameters:
- A local path to a truststore file
- A local path to a keystore file
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

## Google Dataflow Template

### Setting Up Project Environment

#### Pipeline variables:

```
PROJECT=id-of-my-project
BUCKET_NAME=my-bucket
REGION=my-region
```

#### Template Metadata Storage Bucket Creation

The Dataflow Flex template has to store its metadata in a bucket in
[Google Cloud Storage](https://cloud.google.com/storage), so it can be executed from the Google Cloud Platform.
Create the bucket in Google Cloud Storage if it doesn't exist yet:

```
gsutil mb gs://${BUCKET_NAME}
```

#### Containerization variables:

```
IMAGE_NAME=my-image-name
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
BASE_CONTAINER_IMAGE=my-base-container-image
TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/kafka-pubsub.json"
```

### Creating the Dataflow Flex Template

Dataflow Flex Templates package the pipeline as a Docker image and stage these images
on your project's [Container Registry](https://cloud.google.com/container-registry).

To execute the template you need to create the template spec file containing all
the necessary information to run the job. This template already has the following
[metadata file](kafka-to-pubsub/src/main/resources/kafka_to_pubsub_metadata.json) in resources.

Navigate to the template folder:

```
cd /path/to/beam/examples/templates/java/kafka-to-pubsub
```

Build the Dataflow Flex Template:

```
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
       --image-gcr-path ${TARGET_GCR_IMAGE} \
       --sdk-language "JAVA" \
       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
       --metadata-file "src/main/resources/kafka_to_pubsub_metadata.json" \
       --jar "build/libs/beam-examples-templates-java-kafka-to-pubsub-2.25.0-SNAPSHOT-all.jar" \
       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.beam.templates.KafkaToPubsub"
```

### Create Dataflow Job Using the Apache Kafka to Google Pub/Sub Dataflow Flex Template

To deploy the pipeline, you should refer to the template file and pass the
[parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options)
required by the pipeline.

You can do this in 3 different ways:
1. Using [Dataflow Google Cloud Console](https://console.cloud.google.com/dataflow/jobs)

2. Using `gcloud` CLI tool
    ```
    gcloud dataflow flex-template run "kafka-to-pubsub-`date +%Y%m%d-%H%M%S`" \
        --template-file-gcs-location "${TEMPLATE_PATH}" \
        --parameters bootstrapServers="broker_1:9092,broker_2:9092" \
        --parameters inputTopics="topic1,topic2" \
        --parameters outputTopic="projects/${PROJECT}/topics/your-topic-name" \
        --parameters outputFormat="PLAINTEXT" \
        --parameters secretStoreUrl="http(s)://host:port/path/to/credentials" \
        --parameters vaultToken="your-token" \
        --region "${REGION}"
    ```
3. With a REST API request
    ```
    API_ROOT_URL="https://dataflow.googleapis.com"
    TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
    JOB_NAME="kafka-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"

    time curl -X POST -H "Content-Type: application/json" \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        -d '
         {
             "launch_parameter": {
                 "jobName": "'$JOB_NAME'",
                 "containerSpecGcsPath": "'$TEMPLATE_PATH'",
                 "parameters": {
                     "bootstrapServers": "broker_1:9091, broker_2:9092",
                     "inputTopics": "topic1, topic2",
                     "outputTopic": "projects/'$PROJECT'/topics/your-topic-name",
                     "outputFormat": "PLAINTEXT",
                     "secretStoreUrl": "http(s)://host:port/path/to/credentials",
                     "vaultToken": "your-token"
                 }
             }
         }
        '
        "${TEMPLATES_LAUNCH_API}"
    ```

## AVRO format transferring.
This template contains an example Class to deserialize AVRO from Kafka and serialize it to AVRO in Pub/Sub.

To use this example in the specific case, follow these steps:

- Create your own class to describe AVRO schema. As an example use [TaxiRide class](kafka-to-pubsub/src/main/java/org/apache/beam/templates/avro/TaxiRide.java). Just define necessary fields.
- Create your own Avro Deserializer class. As an example use [TaxiRidesKafkaAvroDeserializer class](kafka-to-pubsub/src/main/java/org/apache/beam/templates/avro/TaxiRidesKafkaAvroDeserializer.java). Just rename it, and put your own Schema class as the necessary types.
- Modify the [FormatTransform.readAvrosFromKafka method](kafka-to-pubsub/src/main/java/org/apache/beam/templates/transforms/FormatTransform.java). Put your Schema class and Deserializer to the related parameter.
```java
return KafkaIO.<String, TaxiRide>read()
        ...
        .withValueDeserializerAndCoder(
            TaxiRidesKafkaAvroDeserializer.class, AvroCoder.of(TaxiRide.class)) // put your classes here
        ...
```
- Modify the write step in the [KafkaToPubsub class](kafka-to-pubsub/src/main/java/org/apache/beam/templates/KafkaToPubsub.java) by putting your Schema class to "writeAvrosToPubSub" step.
```java
if (options.getOutputFormat() == FormatTransform.FORMAT.AVRO) {
      ...
          .apply("writeAvrosToPubSub", PubsubIO.writeAvros(TaxiRide.class)); // put your SCHEMA class here

    }
```

_Note: The Kafka to Pub/Sub Dataflow Flex template doesn't support SSL configuration._
