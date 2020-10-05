# Dataflow Kafka to Pub/Sub Flex template

This directory contains the template that creates the Dataflow pipeline 
to read data from a single topic or multiple topics from 
[Apache Kafka](https://kafka.apache.org/) and write it into a single topic 
in [Google Pub/Sub](https://cloud.google.com/pubsub).

## Requirements

- Java 11
- Kafka Bootstrap Server(s) up and running
- Kafka Topic(s) exists
- The PubSub output topic exists

## Getting Started

This section describes the preparation steps that need to be done before 
the template build and execution.

### Setting Environment Variables

Pipeline variables:

```
PROJECT=id-of-my-project
BUCKET_NAME=my-bucket
REGION=my-region
```

Containerization variables:

```
IMAGE_NAME=my-image-name
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
BASE_CONTAINER_IMAGE=my-base-container-image
BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/kafka-pubsub.json"
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
```

### Bucket Creation

The Dataflow Flex templates has to store its metadata in a bucket in 
Google Cloud Storage, so it can be executed from the Google Cloud Platform.
Create the bucket in Google Cloud Storage if it doesn't exist yet:

```
gsutil mb gs://${BUCKET_NAME}
```

## The Dataflow Flex Template

Flex Templates package the pipeline as a Docker image and stage these images 
on your project's Container Registry.

### Assembling the Uber-JAR

The Dataflow Flex templates require your Java project to be built into 
an Uber JAR file.

Go to the Beam folder:

```
cd /path/to/beam
```

In order to create Uber JAR with Gradle, [Shadow plugin](https://github.com/johnrengelman/shadow) 
is used. It creates the `shadowJar` task that builds the Uber JAR:

```
./gradlew -p templates/kafka-to-pubsub clean shadowJar
```

ℹ️ An **Uber JAR** - also known as **fat JAR** - is a single JAR file that contains 
both target package *and* all its dependencies.

The result of the `shadowJar` task execution is a `.jar` file that is generated 
under the `build/libs/` folder in kafka-to-pubsub directory.

### Creating the Flex Template

To execute the template you need to create the template spec file containing all
the necessary information to run the job. This template already has such [metadata
file](kafka-to-pubsub/src/main/resources/kafka_to_pubsub_metadata.json) in resources.

Go to the template folder:

```
cd /path/to/beam/templates/kafka-to-pubsub
```

Build the Flex Template:

```
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
       --image-gcr-path "{$TARGET_GCR_IMAGE}" \
       --sdk-language "JAVA" \
       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
       --metadata-file "src/main/resources/kafka_to_pubsub_metadata.json" \
       --jar "build/libs/beam-templates-kafka-to-pubsub-2.25.0-SNAPSHOT-all.jar" \
       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.apache.beam.templates.KafkaToPubsub"
```

### Running the Pipeline

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
        --region "${REGION}"
    ```
3. With a REST API request
    ```
    API_ROOT_URL="https://dataflow.googleapis.com"
    TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
    JOB_NAME="kafka-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"
    
    time curl -X POST -H "Content-Type: application/json" \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        "${TEMPLATES_LAUNCH_API}"`
        `"?validateOnly=false"`
        `"&dynamicTemplate.gcsPath=${BUCKET_NAME}/path/to/image-spec"`
        `"&dynamicTemplate.stagingLocation=${BUCKET_NAME}/staging" \
        -d '
         {
          "jobName":"${JOB_NAME}",
          "parameters": {
              "bootstrapServers":"broker_1:9092,broker_2:9092",
              "inputTopics":"topic1,topic2",
              "outputTopic":"projects/${PROJECT}/topics/your-topic-name"
          }
         }
        '
    ```


