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
    * [Streaming Sentiment Analysis](#streaming-sentiment-analysis)

<!-- TOC -->

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
gcloud storage cp /path/to/UScomments.csv gs://YOUR_BUCKET/UScomments.csv
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
Vertex AI, use the [notebook](
https://github.com/GoogleCloudPlatform/vertex-ai-samples/blob/main/notebooks/community/model_garden/model_garden_huggingface_pytorch_inference_deployment.ipynb).

BigQuery is the pipeline's sink for the inference result output.
A BigQuery dataset needs to exist first before the pipeline can
create/write to a table. See [here](
https://cloud.google.com/bigquery/docs/datasets) for how to create
BigQuery datasets.

The pipeline first reads the YouTube comments .csv dataset from
GCS bucket and performs some clean-up before writing it to a Kafka 
topic. The pipeline then reads from that Kafka topic and applies
various transformation logic before `RunInference` transform performs
remote inference with the Vertex AI model handler and DistilBERT
deployed to a Vertex AI endpoint. The inference result is then
parsed and written to a BigQuery table. 

Run the pipeline (remove the necessary variables in the command):

```sh
export PROJECT="$(gcloud config get-value project)"
export TEMP_LOCATION="gs://YOUR-BUCKET/tmp"
export REGION="us-central1"
export JOB_NAME="streaming-sentiment-analysis-`date +%Y%m%d-%H%M%S`"
export NUM_WORKERS="1"

python -m apache_beam.yaml.main \
  --yaml_pipeline_file transforms/ml/inference/streaming_sentiment_analysis.yaml \
  --runner DataflowRunner \
  --temp_location $TEMP_LOCATION \
  --project $PROJECT \
  --region $REGION \
  --num_workers $NUM_WORKERS \
  --job_name $JOB_NAME \
  --jinja_variables '{ "GCS_PATH": "gs://YOUR-BUCKET/USComments.csv",
  "BOOTSTRAP_SERVERS": "BOOTSTRAP_IP_ADD:9092", 
  "TOPIC": "YOUR_TOPIC", "USERNAME": "KAFKA_USERNAME", "PASSWORD": "KAFKA_PASSWORD", 
  "ENDPOINT": "ENDPOINT_ID", "PROJECT": "PROJECT_ID", "LOCATION": "LOCATION",
  "DATASET": "DATASET_ID", "TABLE": "TABLE_ID" }'
```



