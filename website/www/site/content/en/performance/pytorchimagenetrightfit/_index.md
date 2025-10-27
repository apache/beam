---
title: "PyTorch Language Modeling BERT base Performance"
---

<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# PyTorch Image Classification EfficientNet-B0 Streaming (Right-fitting)

**Model**: PyTorch Image Classification — EfficientNet-B0 (pretrained on ImageNet)
**Accelerator**: Tesla T4 GPU (right-fitted batch size 64 → 32 → 16 → 8)
**Host**: 50 × n1-standard-4 (4 vCPUs, 15 GB RAM)

This streaming pipeline performs image classification using an open-source PyTorch EfficientNet-B0 model optimized for T4 GPUs.
It reads image URIs from Pub/Sub, decodes and preprocesses them in parallel, and runs inference with adaptive batch sizing for optimal GPU utilization.
The pipeline ensures exactly-once semantics via stateful deduplication and idempotent BigQuery writes, allowing stable and reproducible performance measurements under continuous load.

The following graphs show various metrics when running PyTorch Sentiment Analysis Streaming using Hugging Face DistilBERT base uncased model pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_imagenet_rightfit.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="pytorchimagenetrightfit" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="pytorchimagenetrightfit" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="pytorchimagenetrightfit" read_or_write="write" section="date" >}}
