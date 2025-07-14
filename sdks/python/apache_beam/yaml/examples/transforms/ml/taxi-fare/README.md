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

## Streaming Taxi Fare Prediction Pipeline

The example leverages the `RunInference` transform with Vertex AI
model handler [VertexAIModelHandlerJSON](
https://beam.apache.org/releases/pydoc/current/apache_beam.yaml.yaml_ml#apache_beam.yaml.yaml_ml.VertexAIModelHandlerJSONProvider),
in addition to PubSub, Kafka and BiqQuery IO to demonstrate an end-to-end
example of a streaming ML pipeline predicting NYC taxi fare amounts.

The data stream of NYC taxi ride events are from the existing public
PubSub topic `projects/pubsub-public-data/topics/taxirides-realtime`.

For setting up Kafka, an option is to use [Click to Deploy](
https://console.cloud.google.com/marketplace/details/click-to-deploy-images/kafka?)
to quickly launch a Kafka cluster on GCE. See [existing example here](
../../../README.md#kafka) for more context around using Kafka with Beam
and Dataflow.

BigQuery is the pipeline's sink for the inference result output.
A BigQuery dataset needs to exist first before the pipeline can
create/write to a table. Run the following command to create
a BigQuery dataset:

```sh
bq --location=us-central1 mk \
  --dataset DATASET_ID
```
See also [here](
https://cloud.google.com/bigquery/docs/datasets) for more details on
how to create BigQuery datasets

A trained model hosted on Vertex AI is needed before being able to use
the Vertex AI model handler. To train and deploy a custom model for the
taxi fare prediction problem, open and run this [notebook](
custom_nyc_taxifare_model_deployment.ipynb) in Colab Enterprise.

The pipeline first reads the data stream of taxi rides events from the
public PubSub topic and performs some transformations before writing it
to a Kafka topic. The pipeline then reads from that Kafka topic and applies
the necessary transformation logic, before `RunInference` transform performs
remote inference with the Vertex AI model handler and the custom-trained
model deployed to a Vertex AI endpoint. The inference result is then
parsed and written to a BigQuery table.

Run the pipeline (replace with appropriate variables in the command
below):

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://YOUR-BUCKET/tmp"
export REGION="us-central1"
export JOB_NAME="streaming-taxiride-prediction`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="3"

python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/ml/taxi-fare/streaming_sentiment_analysis.yaml \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --jinja_variables '{ "BOOTSTRAP_SERVERS": "BOOTSTRAP_IP_ADD:9092",
  "TOPIC": "YOUR_TOPIC", "USERNAME": "KAFKA_USERNAME", "PASSWORD": "KAFKA_PASSWORD",
  "ENDPOINT": "ENDPOINT_ID", "PROJECT": "PROJECT_ID", "LOCATION": "LOCATION",
  "DATASET": "DATASET_ID", "TABLE": "TABLE_ID" }'
```
