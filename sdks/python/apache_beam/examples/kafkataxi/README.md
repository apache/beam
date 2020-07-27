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

# Python KafkaIO Example

This example reads from the Google Cloud Pub/Sub NYC Taxi stream described
[here](https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon), writes
to a given Kafka topic, and reads back from the same Kafka topic. This example
uses cross-language transforms available in
[kafka.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/kafka.py).
Transforms are implemented in Java and are available
[here](https://github.com/apache/beam/blob/master/sdks/java/io/kafka/src/main/java/org/apache/beam/sdk/io/kafka/KafkaIO.java).

## Prerequisites

Install [Java Development kit (JDK) version 8](https://www.oracle.com/java/technologies/javase-downloads.html)
in your system and make sure that `JAVA_HOME` environment variable points to
your JDK installation. Make sure that `java` command is available in
the environment.

```sh
java -version
<Should print information regarding the installed Java version>
```

## Setup the Kafka cluster

This example requires users to setup a Kafka cluster that the Beam runner
executing the pipeline has access to.

See [here]((https://kafka.apache.org/quickstart)) for general instructions on
setting up a Kafka cluster. One option is to setup the Kafka cluster in
[GCE](https://cloud.google.com/compute). See
[here](https://github.com/GoogleCloudPlatform/java-docs-samples/tree/master/dataflow/flex-templates/kafka_to_bigquery)
for step by step instructions on  setting up a single node Kafka cluster in GCE.
When using Dataflow consider starting the Kafka cluster in the region where
Dataflow pipeline will be running. See
[here](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints)
for more details regarding the selecting a GCP region for Dataflow.

Let's assume that that IP address of one of the [bootstrap servers](https://kafka.apache.org/quickstart)
of the Kafka cluster to be  `123.45.67.89:123` and the port to be `9092`.

```sh
export BOOTSTRAP_SERVER="123.45.67.89:123:9092"
```

## Running the example on latest released Beam version

Perform Beam runner specific setup.

ℹ️ Note that cross-language transforms require
portable implementations of Spark/Flink/Direct runners. Dataflow requires
[runner V2](https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#dataflow-runner-v2).
See [here](https://beam.apache.org/documentation/runners/dataflow/) for
instructions for setting up Dataflow.

Setup a virtual environment for running Beam Python programs. See
[here](https://beam.apache.org/get-started/quickstart-py/) for prerequisites.
Dataflow requires the `gcp` tag when installing Beam.

```sh
python -m venv env
source env/bin/activate
pip install 'apache-beam[gcp]'
```

Run the Beam pipeline. You can either use the default Kafka topic name or
specify a Kafka topic name. Following command assumes Dataflow. See
[here](https://beam.apache.org/get-started/quickstart-py/) for instructions on
running Beam Python programs on other runners.

ℹ️ Note that this exemple is not available in Beam versions before 2.24.0 hence
you'll have to either get the example program from Beam or follow steps
provided in the section *Running the Example from a Beam Git Clone*.

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://MY-BUCKET/temp"
export REGION="us-central1"
export JOB_NAME="kafka-taxi-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="5"

python -m apache_beam.examples.kafkataxi.kafka_taxi \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --bootstrap_servers $BOOTSTRAP_SERVER \
  --experiments=use_runner_v2
```

## *(Optional)*  Running the Example from a Beam Git Clone

Running this example from a Beam Git clone requires some additional steps.

Checkout a clone of the Beam Git repo. See
[here](https://beam.apache.org/contribute/) for prerequisites.

Assume your Github username to be `GITHUB_USERNAME`.

```sh
git clone git@github.com:${GITHUB_USERNAME}/beam
cd beam
```

Build IO expansion service jar.

```sh
./gradlew :sdks:java:io:expansion-service:build
```

Push a java SDK Harness container to [Docker](https://www.docker.com/get-started)
Hub. See
[here](https://beam.apache.org/documentation/runtime/environments/) for
prerequisites and additional information.

```sh
export DOCKER_ROOT="Your Docker Repository Root"
./gradlew :sdks:java:container:docker -Pdocker-repository-root=$DOCKER_ROOT -Pdocker-tag=latest
docker push $DOCKER_ROOT/beam_java_sdk:latest
```

For portable Flink/Spark in local mode, instead of above command just build the
Java SDK harness container locally using the default values for repository root
and the docker tag.

Activate your Python virtual environment.  This example uses `venv`. See
[here](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips) for
instructions regarding setting up other types of Python virtual environments.

```sh
cd ..  # Creating the virtual environment in the top level work directory.
python -m venv env
source env/bin/activate
```

Install Beam and dependencies and build a Beam distribution.

```sh
cd beam/sdks/python
pip install -r build-requirements.txt
pip install -e '.[gcp]'
python setup.py sdist
```

Run the Beam pipeline. You can either use the default Kafka topic name or specify
a Kafka topic name. Following command assumes Dataflow. See
[here](https://beam.apache.org/get-started/quickstart-py/) for instructions on
running Beam Python programs on other runners.

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://MY-BUCKET/temp"
export REGION="us-central1"
export JOB_NAME="kafka-taxi-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="5"
export PYTHON_DISTRIBUTION="dist/'Name of Python distribution'"

python -m apache_beam.examples.kafkataxi.kafka_taxi \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --sdk_location $PYTHON_DISTRIBUTION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --bootstrap_servers $BOOTSTRAP_SERVER \
  --sdk_harness_container_image_overrides ".*java.*,${DOCKER_ROOT}/beam_java_sdk:latest" \
  --experiments=use_runner_v2
```
