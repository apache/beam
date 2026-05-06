---
title: "MLTransform Generate Vocab Performance"
---

<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
-->

# MLTransform Generate Vocab Performance

**Model**: Apache Beam MLTransform — ComputeAndApplyVocabulary (Generate Vocab) + TFIDF
**Accelerator**: CPU (batch)
**Host**: MLTransform vocab pipeline on Dataflow (batch)

This batch pipeline computes a vocabulary from input text columns using
`ComputeAndApplyVocabulary`, then produces TF-IDF outputs using `TFIDF`.

See the [glossary](/performance/glossary) for definitions.

Full pipeline implementation is available here:
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/ml_transform/mltransform_generate_vocab.py

## What is the estimated cost to run the pipeline?

{{< performance_looks io="mltransformvocab" read_or_write="write" section="cost" >}}

## How has various metrics changed when running the pipeline for different Beam SDK versions?

{{< performance_looks io="mltransformvocab" read_or_write="write" section="version" >}}

## How has various metrics changed over time when running the pipeline?

{{< performance_looks io="mltransformvocab" read_or_write="write" section="date" >}}

