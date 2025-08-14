---
title: "VLLM Gemma 2b Batch Performance on Tesla T4"
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# VLLM Gemma 2b Batch Performance on Tesla T4

**Model**: google/gemma-2b-it
**Accelerator**: NVIDIA Tesla T4 GPU
**Host**: 3 × n1-standard-8 (8 vCPUs, 30 GB RAM)

The following graphs show various metrics when running VLLM Gemma 2b Batch Performance on Tesla T4 pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/vllm_gemma_batch.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="vllmgemmabatchtesla" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="vllmgemmabatchtesla" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="vllmgemmabatchtesla" read_or_write="write" section="date" >}}
