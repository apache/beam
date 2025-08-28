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
        * [Jinja](#jinja)
        * [ML](#ml)

<!-- TOC -->

## Prerequistes

Build this jar for running with the run command in the next stage:

```
cd <PATH_TO_BEAM_REPO>/beam; ./gradlew sdks:java:io:google-cloud-platform:expansion-service:shadowJar
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

pytest -v testing/examples_test.py::JinjaTest

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
`Spanner_Write` transforms for performing simple reads and writes from a
Google Spanner database.

#### Kafka

Examples involving Kafka such as [Kafka Read Write](transforms/io/kafka.yaml)
require users to set up a Kafka cluster that Dataflow runner executing the
Beam pipeline has access to.
Please note that `ReadFromKafka` transform has
a [known issue](https://github.com/apache/beam/issues/22809) when
using non-Dataflow portable runners where reading may get stuck in streaming
pipelines. Hence using the Dataflow runner is recommended for examples that
involve reading from Kafka in a streaming pipeline.

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

Beam pipeline [Kafka Read Write](transforms/io/kafka.yaml) first writes data to
the Kafka topic using the `WriteToKafka` transform and then reads that data back
using the `ReadFromKafka` transform. Run the pipeline:

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

Beam pipelines [Iceberg Write](transforms/io/iceberg_write.yaml) and
[Iceberg Read](transforms/io/iceberg_read.yaml) are examples of how to interact
with Iceberg tables on GCS storage and with Hadoop catalog configured.

To create a GCS bucket as our warehouse storage,
see [here](https://cloud.google.com/storage/docs/creating-buckets#command-line).
To run the pipelines locally, an option is to create a service account key in
order to access GCS (see
[here](https://cloud.google.com/iam/docs/keys-create-delete#creating)).
Within the pipelines, specify GCS bucket name and the path to the saved service
account key .json file.

**_Note_**: With Hadoop catalog, Iceberg will use Hadoop connector for GCS.
See [here](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
for full list of configuration options for Hadoop catalog when use with GCS.

To create and write to Iceberg tables on GCS, run:

```sh
python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/io/iceberg_write.yaml
```

The pipeline uses [Dynamic destinations](
https://cloud.google.com/dataflow/docs/guides/managed-io#dynamic-destinations)
write to dynamically create and select a table destination based on field
values in the incoming records.

To read from a created Iceberg table on GCS, run:

```sh
python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/io/iceberg_read.yaml
```

**_Optional_**: To run the pipeline on Dataflow, service account key is
[not needed](
https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/INSTALL.md).
Omit the authentication settings in the Hadoop catalog configuration `
config_properties`, and run:

```sh
export REGION="us-central1"
export JOB_NAME="demo-iceberg_write-`date +%Y%m%d-%H%M%S`"

gcloud dataflow yaml run $JOB_NAME \
  --yaml-pipeline-file transforms/io/iceberg_write.yaml \
  --region $REGION
```

```sh
export REGION="us-central1"
export JOB_NAME="demo-iceberg_read-`date +%Y%m%d-%H%M%S`"

gcloud dataflow yaml run $JOB_NAME \
  --yaml-pipeline-file transforms/io/iceberg_read.yaml \
  --region $REGION
```

### Jinja

Jinja [templatization](https://beam.apache.org/documentation/sdks/yaml/#jinja-templatization)
can be used to build off of different contexts and/or with different
configurations.

Several examples will be created based on the already used word count example
by leveraging Jinja templating engine for dynamic pipeline generation based on
inputs from the user through `% include`, `% import`, and inheritance
directives.

Jinja `% import` directive:
- [wordCountImport.yaml](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/jinja/import/wordCountImport.yaml)
- [Instructions](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/jinja/import/README.md) on how to run the pipeline.

Jinja `% include` directive:
- [wordCountInclude.yaml](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/jinja/include/wordCountInclude.yaml)
- [Instructions](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/jinja/include/README.md) on how to run the pipeline.


### ML

Examples that include the built-in `Enrichment` transform for performing
ML enrichments:
- [enrich_spanner_with_bigquery.yaml](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/enrich_spanner_with_bigquery.yaml)
- [bigtable_enrichment.yaml](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/bigtable_enrichment.yaml)

Examples that include ML-specific transforms such as `RunInference` and
`MLTransform`:
- Streaming Sentiment Analysis ([documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/sentiment_analysis)) ([pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/sentiment_analysis/streaming_sentiment_analysis.yaml))
- Streaming Taxi Fare Prediction ([documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/taxi_fare)) ([pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/taxi_fare/streaming_taxifare_prediction.yaml))
- Batch Log Analysis ML Workflow ([documentation](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/yaml/examples/transforms/ml/log_analysis)) ([pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/ml/log_analysis/batch_log_analysis.yaml))

More information can be found about aggregation transforms
[here](https://beam.apache.org/documentation/sdks/yaml-combine/).
