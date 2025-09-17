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

## Streaming Sentiment Analysis

The example leverages the `RunInference` transform with Vertex AI
model handler [VertexAIModelHandlerJSON](
https://beam.apache.org/releases/pydoc/current/apache_beam.yaml.yaml_ml#apache_beam.yaml.yaml_ml.VertexAIModelHandlerJSONProvider),
in addition to Kafka IO to demonstrate an end-to-end example of a
streaming sentiment analysis pipeline. The dataset to perform
sentiment analysis on is the YouTube video comments and can be found
on Kaggle [here](
https://www.kaggle.com/datasets/datasnaek/youtube?select=UScomments.csv).

Download the dataset and copy over to a GCS bucket:
```sh
export GCS_PATH="gs://YOUR-BUCKET/USComments.csv"
gcloud storage cp /path/to/UScomments.csv $GCS_PATH
```

For setting up Kafka, an option is to use [Click to Deploy](
https://console.cloud.google.com/marketplace/details/click-to-deploy-images/kafka?)
to quickly launch a Kafka cluster on GCE. See [here](
../../../README.md#kafka) for more context around using Kafka
with Dataflow.

A hosted model on Vertex AI is needed before being able to use
the Vertex AI model handler. One of the current state-of-the-art
NLP models is HuggingFace's DistilBERT, a distilled version of
BERT model and is faster at inference. To deploy DistilBERT on
Vertex AI, run this [notebook](
https://github.com/GoogleCloudPlatform/vertex-ai-samples/blob/main/notebooks/community/model_garden/model_garden_huggingface_pytorch_inference_deployment.ipynb) in Colab Enterprise.

BigQuery is the pipeline's sink for the inference result output.
A BigQuery dataset needs to exist first before the pipeline can
create/write to a table. Run the following command to create
a BigQuery dataset:

```sh
bq --location=us-central1 mk \
  --dataset DATASET_ID
export BQ_TABLE_ID="PROJECT_ID:DATASET_ID.TABLE_ID"
```
See also [here](
https://cloud.google.com/bigquery/docs/datasets) for more details on
how to create BigQuery datasets

The pipeline first reads the YouTube comments .csv dataset from
GCS bucket and performs some clean-up before writing it to a Kafka
topic. The pipeline then reads from that Kafka topic and applies
various transformation logic before `RunInference` transform performs
remote inference with the Vertex AI model handler and DistilBERT
deployed to a Vertex AI endpoint. The inference result is then
parsed and written to a BigQuery table.

Run the pipeline (replace with appropriate variables in the command
below):

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://YOUR-BUCKET/tmp"
export REGION="us-central1"
export JOB_NAME="streaming-sentiment-analysis-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="3"
export KAFKA_BOOTSTRAP_SERVERS="BOOTSTRAP_IP_ADD:9092"
export KAFKA_TOPIC="YOUR_TOPIC"
export KAFKA_USERNAME="KAFKA_USERNAME"
export KAFKA_PASSWORD="KAFKA_PASSWORD"
export VERTEXAI_ENDPOINT="ENDPOINT_ID"

python -m apache_beam.yaml.main \
  --yaml_pipeline_file streaming_sentiment_analysis.yaml \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --jinja_variables '{ "GCS_PATH": "'$GCS_PATH'",
  "BOOTSTRAP_SERVERS": "'$KAFKA_BOOTSTRAP_SERVERS'",
  "TOPIC": "'$KAFKA_TOPIC'", "USERNAME": "'$KAFKA_USERNAME'", "PASSWORD": "'$KAFKA_PASSWORD'",
  "ENDPOINT": "'$VERTEXAI_ENDPOINT'", "PROJECT": "'$PROJECT'", "LOCATION": "'$REGION'",
  "BQ_TABLE": "'$BQ_TABLE_ID'" }'
```
