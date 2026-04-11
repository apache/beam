---
title: "Table Row Inference Sklearn Batch Performance"
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

# Table Row Inference Sklearn Batch

**Model**: Scikit-learn classifier on structured table data (Beam.Row)
**Accelerator**: CPU-based inference (fixed batch size)
**Host**: 10 × n1-standard-4 (4 vCPUs, 15 GB RAM)

This batch pipeline performs inference on continuous table rows using RunInference with a Scikit-learn model.
It reads structured data (table rows) from GCS in JSONL format, extracts specified feature columns, and runs batched inference while preserving the original table schema.
The pipeline ensures exactly-once semantics within batch execution by deduplicating inputs and writing results to BigQuery using file-based loads, enabling reproducible and comparable performance measurements across runs.

The following graphs show various metrics when running Table Row Inference Sklearn Batch pipeline.
See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available [here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/inference/table_row_inference.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="tablerowinference" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="tablerowinference" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="tablerowinference" read_or_write="write" section="date" >}}

See also [Table Row Inference Sklearn Streaming](/performance/tablerowinferencestreaming) for the streaming variant of this pipeline.

