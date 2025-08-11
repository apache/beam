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

## Batch Log Analysis ML Workflow

This example contains several pipelines that leverage Iceberg and BigQuery
IOs as well as MLTransform to demonstrate an end-to-end ML anomaly detection
workflow on system logs.

Install additional required Python dependencies:
```sh
pip install tensorflow-transform db-types -r requirements.txt
```
**NOTE**:
- The underlying `tensorflow-transform` library that MLTransform uses cannot be
installed on Apple Silicon. This example therefore should be run on
x86 machines.
- This example requires the GCS bucket created to be single-region bucket.

The system logs dataset is from [logpai/loghub](https://github.com/logpai/loghub)
GitHub repository, and specifically the [sample HDFS `.csv` dataset](
https://github.com/logpai/loghub/blob/master/Hadoop/Hadoop_2k.log_structured.csv)
is used in this example.

Download the dataset and copy it over to a GCS bucket:
```sh
gcloud storage cp /path/to/Hadoop_2k.log_structured.csv \
  gs://YOUR_BUCKET/Hadoop_2k.log_structured.csv
```

For Iceberg table, GCS is used as the storage layer. A natural choice for
catalog with data lakehouse on GCS is [BigLake metastore](
https://cloud.google.com/bigquery/docs/about-blms). It is a managed, serverless
metastore that doesn't require any setup.

A BigQuery dataset needs to exist first before the pipeline can
create/write to a table. Run the following command to create
a BigQuery dataset:

```sh
bq --location=YOUR_REGION mk \
  --dataset YOUR_DATASET
```

The workflow starts with pipeline [iceberg_migration](./iceberg_migration.yaml)
that ingests the `.csv` log data and writes to an Iceberg table on GCS with
BigLake metastore for catalog.
The next pipeline [ml_preprocessing](
./ml_preprocessing.yaml) reads from this Iceberg table and perform ML-specific
transformations such as computing text embedding and normalization, before
writing the vector embeddings to a BigQuery table.
An anomaly detection model is then trained (in [train.py](./train.py) script)
on these vector embeddings, and the trained model is saved as artifact on GCS.
The last pipeline [anomaly_scoring](./anomaly_scoring.yaml) reads the vector
embeddings from the BigQuery table, uses Beam's anomaly detection module to
load the model artifact from GCS and perform anomaly scoring, before writing
it to another Iceberg table.

This entire workflow execution is encapsulated in the `batch_log_analysis.sh`
script that runs these workloads sequentially.

Run the workflow locally:
```sh
./batch_log_analysis.sh --runner DirectRunner \
  --project YOUR_PROJECT \
  --region YOUR_REGION \
  --warehouse gs://YOUR-BUCKET \
  --bq_table YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE
```

Run the workflow on Dataflow:
```sh
 ./batch_log_analysis.sh --runner DataflowRunner \
   --project YOUR_PROJECT \
   --region YOUR_REGION \
   --temp_location gs://YOUR-BUCKET/tmp \
   --num_workers 1 \
   --worker_machine_type n1-standard-4 \
   --warehouse gs://YOUR-BUCKET \
   --bq_table YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE
```
