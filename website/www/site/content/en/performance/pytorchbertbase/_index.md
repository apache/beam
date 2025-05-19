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

# PyTorch Language Modeling BERT base Performance

**Model**: PyTorch Language Modeling — BERT (base-uncased)
**Accelerator**: CPU only
**Host**: 250 × n1-standard-2 (2 vCPUs, 7.5 GB RAM)

The following graphs show various metrics when running Pytorch Language Modeling using Hugging Face bert-base-uncased model pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/pytorch_language_modeling.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="pytorchbertbase" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="pytorchbertbase" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="pytorchbertbase" read_or_write="write" section="date" >}}
