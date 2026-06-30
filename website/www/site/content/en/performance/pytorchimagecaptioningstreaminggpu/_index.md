---
title: "PyTorch Image Captioning BLIP + CLIP Streaming GPU Performance"
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

# PyTorch Image Captioning BLIP + CLIP Streaming GPU

**Model**: PyTorch Image Captioning — BLIP (candidate generation) + CLIP (ranking)
**Accelerator**: Tesla T4 GPU
**Host**: 50 × n1-standard-4 (4 vCPUs, 15 GB RAM)

This streaming pipeline performs image captioning using a multi-model open-source PyTorch approach.
It first generates multiple candidate captions per image using a BLIP model, then ranks these candidates with a CLIP model based on image-text similarity.

The following graphs show various metrics when running PyTorch Image Captioning BLIP + CLIP Streaming GPU pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_image_captioning.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="pytorchimagecaptioningstreaminggpu" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="pytorchimagecaptioningstreaminggpu" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="pytorchimagecaptioningstreaminggpu" read_or_write="write" section="date" >}}
