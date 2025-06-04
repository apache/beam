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

# Examples Catalog

<!-- TOC -->

* [Examples Catalog](#examples-catalog)
    * [Wordcount](#wordcount)
    * [Transforms](#transforms)
        * [Aggregation](#aggregation)
        * [Blueprints](#blueprints)
        * [Element-wise](#element-wise)
        * [IO](#io)
        * [ML](#ml)

<!-- TOC -->

## Prerequistes

Build this jar for running with the run command in the next stage:

```
cd <path_to_beam_repo>/beam; ./gradlew sdks:java:io:google-cloud-platform:expansion-service:shadowJar
```

## Example Run

This module contains a series of Beam YAML code samples that can be run using
the command:

```
python -m apache_beam.yaml.main --yaml_pipeline_file=/path/to/example.yaml
```

Depending on the yaml pipeline, the output may be emitted to standard output or
a file located in the execution folder used.

## Wordcount

A good starting place is the [Wordcount](wordcount_minimal.yaml) example under
the root example directory.
This example reads in a text file, splits the text on each word, groups by each
word, and counts the occurrence of each word. This is a classic example used in
the other SDK's and shows off many of the functionalities of Beam YAML.

## Testing

A test file is located in the testing folder that will execute all the example
yamls and confirm the expected results.

```
pytest -v testing/

or

python -m unittest -v testing/examples_test.py
```

## Transforms

Examples in this directory show off the various built-in transforms of the Beam
YAML framework.

### Aggregation

These examples leverage the built-in `Combine` transform for performing simple
aggregations including sum, mean, count, etc.

### Blueprints

These examples leverage DF or other existing templates and convert them to yaml
blueprints.

### Element-wise

These examples leverage the built-in mapping transforms including `MapToFields`,
`Filter` and `Explode`. More information can be found about mapping transforms
[here](https://beam.apache.org/documentation/sdks/yaml-udf/).

### IO

#### Spanner

Examples [Spanner Read](transforms/io/spanner_read.yaml) and [Spanner Write](
transforms/io/spanner_write.yaml) leverage the built-in `Spanner_Read` and
`Spanner_Write` transform for performing simple reads and writes from a spanner
DB.

#### Kafka

Examples involving Kafka such as [Streaming Wordcount](streaming_wordcount.yaml)
and [Kafka Read Write](transforms/io/kafka.yaml) require users to set up
a Kafka cluster that Dataflow runner executing the
Beam pipeline has access to. See
issue [here](https://github.com/apache/beam/issues/22809) for context on running
streaming pipelines with Kafka on Dataflow runner vs. portable runners.

See [here](https://kafka.apache.org/quickstart) for general instructions on
setting up a Kafka cluster. An option is to use [Click to Deploy](
https://console.cloud.google.com/marketplace/details/click-to-deploy-images/kafka?)
to quickly launch a Kafka cluster on [GCE](
https://cloud.google.com/products/compute?hl=en). [SASL/PLAIN](
https://kafka.apache.org/documentation/#security_sasl_plain) authentication
mechanism is configured for the brokers as part of the deployment. See
also [here](
https://github.com/GoogleCloudPlatform/java-docs-samples/tree/main/dataflow/flex-templates/kafka_to_bigquery)
for an alternative step-by-step guide on setting up Kafka on GCE without the
authentication mechanism.

Let's assume one of the bootstrap servers is on VM instance `kafka-vm-0`
with the internal IP address `123.45.67.89` and port `9092` that the bootstrap
server is listening on. SASL/PLAIN `USERNAME` and `PASSWORD` can be viewed from
the VM instance's metadata on the GCE console, or with gcloud CLI:

```sh
gcloud compute instances describe kafka-vm-0 \
  --format='value[](metadata.items.kafka-user)'
gcloud compute instances describe kafka-vm-0 \
  --format='value[](metadata.items.kafka-password)'
```

Beam pipeline [Streaming Wordcount](streaming_wordcount.yaml) reads from an
existing Kafka topic `MY-TOPIC` containing lines of text and then applies
transformation logic similar to [Wordcount](wordcount_minimal.yaml) example,
before finally logs out the output. Run the pipeline:

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://MY-BUCKET/tmp"
export REGION="us-central1"
export JOB_NAME="streaming-wordcount-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="1"

python -m apache_beam.yaml.main \
  --yaml_pipeline_file streaming_wordcount.yaml \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --jinja_variables '{ "BOOTSTRAP_SERVERS": "123.45.67.89:9092",
    "TOPIC": "MY-TOPIC", "USERNAME": "USERNAME", "PASSWORD": "PASSWORD" }'
```

Beam pipeline [Kafka Read Write](transforms/io/kafka.yaml) is a non-linear
pipeline that both writes to and reads from the same Kafka topic. Run the
pipeline:

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://MY-BUCKET/tmp"
export REGION="us-central1"
export JOB_NAME="demo-kafka-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="1"

python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/io/kafka.yaml \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --jinja_variables '{ "BOOTSTRAP_SERVERS": "123.45.67.89:9092",
    "TOPIC": "MY-TOPIC", "USERNAME": "USERNAME", "PASSWORD": "PASSWORD" }'
```

**_Optional_**: If Kafka cluster is set up with no SASL/PLAINTEXT authentication
configured for the brokers, there's no SASL/PLAIN `USERNAME` and `PASSWORD`
needed. In the pipelines, omit the configurations `producer_config_updates` and
`consumer_config` from the `WriteToKafka` and `ReadFromKafka` transforms.
Run the commands above without specifying the username and password in
`--jinja_variables` flag.

#### Iceberg

Beam pipelines [Iceberg Read](transforms/io/iceberg_read.yaml) and
[Iceberg Write](transforms/io/iceberg_write.yaml) are examples of how to
interact with Iceberg tables on GCS storage and with Hadoop catalog configured.

To run the pipelines locally, an option is to
create a service account key in order to access GCS (see
[here](https://cloud.google.com/iam/docs/keys-create-delete#creating)).
Within the pipelines, specify GCS bucket name and the path to the saved service
account key .json file.

**_Note_**: With Hadoop catalog, Iceberg will use Hadoop connector for GCS.
See [here](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
for full list of configuration options for Hadoop catalog when use with GCS.

Run the pipelines:

```sh
python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/io/iceberg_read.yaml
```

```sh
python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/io/iceberg_write.yaml
```

**_Optional_**: To run the pipeline on Dataflow, service account key is [not
needed](
https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/INSTALL.md
). Omit the authentication settings in the Hadoop catalog configuration `
config_properties`, and run:

```sh
export REGION="us-central1"
export JOB_NAME="demo-iceberg_read-`date +%Y%m%d-%H%M%S`"

gcloud dataflow yaml run $JOB_NAME \
  --yaml-pipeline-file transforms/io/iceberg_read.yaml \
  --region $REGION
```

```sh
export REGION="us-central1"
export JOB_NAME="demo-iceberg_write-`date +%Y%m%d-%H%M%S`"

gcloud dataflow yaml run $JOB_NAME \
  --yaml-pipeline-file transforms/io/iceberg_read.yaml \
  --region $REGION
```

### ML

These examples leverage the built-in `Enrichment` transform for performing
ML enrichments.
