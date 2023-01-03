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
below. Beam's performance over time can be viewed at http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1

## Pytorch RunInference Image Classification 50K

The Pytorch RunInference Image Classification 50K benchmark runs an
[example image classification pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_image_classification.py)
using various different resnet image classification models (the benchmarks on
[Beam's dashboard](http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1)
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

All the performance tests are defined at [job_InferenceBenchmarkTests_Python.groovy](https://github.com/apache/beam/blob/master/.test-infra/jenkins/job_InferenceBenchmarkTests_Python.groovy).
