---
title: "MLTransform Image Embedding CPU Performance"
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

# MLTransform Image Embedding CPU Performance

**Model**: Sentence Transformers — clip-ViT-B-32 (image)
**Accelerator**: CPU with Dataflow Prime right-fitting (16 GB min RAM)
**Host**: Dataflow Prime with throughput-based autoscaling

This batch pipeline reads image URIs from GCS, decodes images with Pillow,
generates image embeddings through `MLTransform` with
`SentenceTransformerEmbeddings(image_model=True)`, and writes results to
BigQuery using batch file loads.

See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available
[here](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/ml_transform/mltransform_image_embedding.py).

## What is the estimated cost to run the pipeline?

{{< performance_looks io="mltransform-image-embedding-cpu" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="mltransform-image-embedding-cpu" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="mltransform-image-embedding-cpu" read_or_write="write" section="date" >}}

See also [MLTransform Image Embedding GPU](/performance/mltransform-image-embedding-gpu)
for the Tesla T4 GPU variant of this pipeline.
