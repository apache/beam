---
title: "PyTorch Image Object Detection Faster R-CNN ResNet-50 Batch Performance"
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

# PyTorch Image Object Detection Faster R-CNN ResNet-50 Batch

**Model**: PyTorch Image Object Detection — Faster R-CNN ResNet-50 FPN (pretrained on COCO)
**Accelerator**: Tesla T4 GPU (fixed batch size)
**Host**: 50 × n1-standard-4 (4 vCPUs, 15 GB RAM)

This batch pipeline performs object detection using an open-source PyTorch Faster R-CNN ResNet-50 FPN model on GPU.
It reads image URIs from GCS, decodes and preprocesses images, and runs batched inference with a fixed batch size to measure stable GPU performance.
The pipeline ensures exactly-once semantics within batch execution by deduplicating inputs and writing results to BigQuery using file-based loads, enabling reproducible and comparable performance measurements across runs.

The following graphs show various metrics when running PyTorch Image Object Detection Faster R-CNN ResNet-50 Batch pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_image_object_detection.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="pytorchimageobjectdetection" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="pytorchimageobjectdetection" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="pytorchimageobjectdetection" read_or_write="write" section="date" >}}
