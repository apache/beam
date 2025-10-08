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

# RunInference Benchmarks

This module contains benchmarks used to test the performance of the RunInference transform
running inference with common models and frameworks. Each benchmark is explained in detail
below. Beam's performance over time can be viewed at https://beam.apache.org/performance/.

All the performance tests are defined at [beam_Inference_Python_Benchmarks_Dataflow.yml](https://github.com/apache/beam/blob/master/.github/workflows/beam_Inference_Python_Benchmarks_Dataflow.yml).

## Pytorch RunInference Image Classification 50K

The Pytorch RunInference Image Classification 50K benchmark runs an
[example image classification pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_image_classification.py)
using various different resnet image classification models (the benchmarks on
[Beam's dashboard](https://metrics.beam.apache.org/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1)
display [resnet101](https://pytorch.org/vision/main/models/generated/torchvision.models.resnet101.html) and [resnet152](https://pytorch.org/vision/stable/models/generated/torchvision.models.resnet152.html))
against 50,000 example images from the OpenImage dataset. The benchmarks produce
the following metrics:

- Mean Inference Requested Batch Size - the average batch size that RunInference groups the images into for batch prediction
- Mean Inference Batch Latency - the average amount of time it takes to perform inference on a given batch of images
- Mean Load Model Latency - the average amount of time it takes to load a model. This is done once per DoFn instance on worker
startup, so the cost is amortized across the pipeline.

These metrics are published to InfluxDB and BigQuery.

### Pytorch Image Classification Tests

* Pytorch Image Classification with Resnet 101.
  * machine_type: n1-standard-2
  * num_workers: 75
  * autoscaling_algorithm: NONE
  * disk_size_gb: 50

* Pytorch Image Classification with Resnet 152.
  * machine_type: n1-standard-2
  * num_workers: 75
  * autoscaling_algorithm: NONE
  * disk_size_gb: 50

* Pytorch Imagenet Classification with Resnet 152 with Tesla T4 GPU.
  * machine_type:
    * CPU: n1-standard-2
    * GPU: NVIDIA Tesla T4
  * num_workers: 75
  * autoscaling_algorithm: NONE
  * disk_size_gb: 50

Approximate size of the models used in the tests
* resnet101: 170.5 MB
* resnet152: 230.4 MB

## Pytorch RunInference Language Modeling

The Pytorch RunInference Language Modeling benchmark runs an
[example language modeling pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py)
using the [Bert large uncased](https://huggingface.co/bert-large-uncased)
and [Bert base uncased](https://huggingface.co/bert-base-uncased) models
and a dataset of 50,000 manually generated sentences. The benchmarks produce
the following metrics:

- Mean Inference Requested Batch Size - the average batch size that RunInference groups the images into for batch prediction
- Mean Inference Batch Latency - the average amount of time it takes to perform inference on a given batch of images
- Mean Load Model Latency - the average amount of time it takes to load a model. This is done once per DoFn instance on worker
startup, so the cost is amortized across the pipeline.

These metrics are published to InfluxDB and BigQuery.

### Pytorch Language Modeling Tests

* Pytorch Langauge Modeling using Hugging Face bert-base-uncased model.
  * machine_type: n1-standard-2
  * num_workers: 250
  * autoscaling_algorithm: NONE
  * disk_size_gb: 50

* Pytorch Langauge Modeling using Hugging Face bert-large-uncased model.
  * machine_type: n1-standard-2
  * num_workers: 250
  * autoscaling_algorithm: NONE
  * disk_size_gb: 50

Approximate size of the models used in the tests
* bert-base-uncased: 417.7 MB
* bert-large-uncased: 1.2 GB

## PyTorch Sentiment Analysis DistilBERT base

**Model**: PyTorch Sentiment Analysis — DistilBERT (base-uncased)
**Accelerator**: CPU only
**Host**: 20 × n1-standard-2 (2 vCPUs, 7.5 GB RAM)

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_sentiment_streaming.py).

## VLLM Gemma 2b Batch Performance on Tesla T4

**Model**: google/gemma-2b-it
**Accelerator**: NVIDIA Tesla T4 GPU
**Host**: 3 × n1-standard-8 (8 vCPUs, 30 GB RAM)

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/vllm_gemma_batch.py).

## How to add a new ML benchmark pipeline

1. Create the pipeline implementation

- Location: sdks/python/apache_beam/examples/inference (e.g., pytorch_sentiment.py)
- Define CLI args and the logic
- Keep parameter names consistent (e.g., --bq_project, --bq_dataset, --metrics_table).

2. Create the benchmark implementation

- Location: sdks/python/apache_beam/testing/benchmarks/inference (e.g., pytorch_sentiment_benchmarks.py)
- Inherit from DataflowCostBenchmark  class.
- Ensure the expected 'pcollection' parameter is passed to your builder. This parameter could be obtained from GCP Dataflow Jobs -> Your Job Page.
- Keep naming consistent with other benchmarks.

3. Add an options txt file

- Location: .github/workflows/load-tests-pipeline-options/<pipeline_name>.txt
- Include Dataflow and pipeline flags. Example:

```
--region=us-central1
--machine_type=n1-standard-2
--num_workers=75
--disk_size_gb=50
--autoscaling_algorithm=NONE
--staging_location=gs://temp-storage-for-perf-tests/loadtests
--temp_location=gs://temp-storage-for-perf-tests/loadtests
--requirements_file=apache_beam/ml/inference/your-requirements-file.txt
--publish_to_big_query=true
--metrics_dataset=beam_run_inference
--metrics_table=your_table
--input_options={}
--influx_measurement=your-measurement
--device=CPU
--runner=DataflowRunner
```

4. Wire it into the GitHub Action

- Workflow: .github/workflows/beam_Inference_Python_Benchmarks_Dataflow.yml
- Add your argument-file-path to the matrix.
- Add a step that runs your <pipeline_name>_benchmarks.py with -PloadTest.args=$YOUR_ARGUMENTS.

5. Test on your fork

- Trigger the workflow manually.
- Confirm the Dataflow job completes successfully.

6. Verify metrics in BigQuery

- Dataset: beam_run_inference. Table: your_table
- Confirm new rows for your pipeline_name with recent timestamps.

7. Update the website

- Create: website/www/site/content/en/performance/<pipeline_name>/_index.md (short title/description).
- Update: website/www/site/data/performance.yaml — add your pipeline and five chart entries with:
- - looker_folder_id
- - public_slug_id (from Looker, see below)

8. Create Looker content (5 charts)

- In Looker → Shared folders → run_inference: create a subfolder for your pipeline.
- From an existing chart: Development mode → Explore from here → Go to LookML.
- Point to your table/view and create 5 standard charts (latency/throughput/cost/etc.).
- Save changes → Publish to production.
- From Explore, open each, set fields/filters for your pipeline, Run, then Save as Look (in your folder).
- Open each Look:
- - Copy Look ID
- - Add Look IDs to .test-infra/tools/refresh_looker_metrics.py.
- - Exit Development mode → Edit Settings → Allow public access.
- - Copy public_slug_id and paste into website/performance.yml.
- - Run .test-infra/tools/refresh_looker_metrics.py script or manually download as PNG via the public slug and upload to GCS: gs://public_looker_explores_us_a3853f40/FOLDER_ID/<look_slug>.png

9. Open a PR
