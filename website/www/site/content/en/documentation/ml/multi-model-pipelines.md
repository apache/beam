---
title: "Multi-model pipelines"
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

# Multi-model pipelines

Apache Beam allows you to develop multi-model pipelines. This example demonstrates how to ingest
and transform input data, run it through a model, and then pass the outcome of your first model
into a second model. This page explains how multi-model pipelines work and gives an overview of what
you need to know to build one.

Before reading this section, it is recommended that you become familiar with the information in
the [Pipeline development lifecycle](/documentation/pipelines/design-your-pipeline/).

## How to build a Multi-model pipeline with Beam

A typical machine learning workflow involves a series of data transformation steps, such as data
ingestion, data processing tasks, inference, and post-processing. Apache Beam enables you to orchestrate
all of those steps together by encapsulating them in a single Apache Beam Directed Acyclic Graph (DAG), which allows you to build
resilient and scalable end-to-end machine learning systems.

To deploy your machine learning model in an Apache Beam pipeline, use
the [`RunInferenceAPI`](/documentation/sdks/python-machine-learning/), which
facilitates the integration of your model as a `PTransform` step in your DAG. Composing
multiple `RunInference` transforms within a single DAG makes it possible to build a pipeline that consists
of multiple ML models. In this way, Apache Beam supports the development of complex ML systems.

You can use different patterns to build multi-model pipelines in Apache Beam. This page explores A/B patterns and cascade patterns.

### A/B Pattern

The A/B pattern describes a framework multiple where ML models are running in parallel. One
application for this pattern is to test the performance of different machine learning models and
decide whether a new model is an improvement over an existing one. This is also known as the
“Champion/Challenger” method. Typically, you define a business metric to compare the performance
of a control model with the current model.

An example could be recommendation engine models where you have an existing model that recommends
ads based on the user’s preferences and activity history. When deciding to deploy a new model, you
could split the incoming user traffic into two branches, where half of the users are exposed to the
new model and the other half to the current one.

After, you can measure the average click-through rate (CTR) of ads for both sets of
users over a defined period of time to determine if the new model is performing better than the
existing one.

```
import apache_beam as beam

with beam.Pipeline() as pipeline:
   userset_a_traffic, userset_b_traffic =
     (pipeline | 'ReadFromStream' >> beam.ReadFromStream('stream_source')
               | ‘Partition’ >> beam.partition(split_dataset, 2, ratio=[5, 5])
     )

model_a_predictions = userset_a_traffic | RunInference(<model_handler_A>)
model_b_predictions = userset_b_traffic | RunInference(<model_handler_B>)
```

Where `beam.partition` is used to split the data source into 50/50 split partitions. For more
information about data partitioning,
see [Partition](/documentation/transforms/python/elementwise/partition/).

### Cascade Pattern

The Cascade pattern is used when the solution to a problem involves a series of ML models. In
this scenario, the output of a model is typically transformed to a suitable format using
a `PTransform` before passing it to another model.

```
with pipeline as p:
   data = p | 'Read' >> beam.ReadFromSource('a_source')
   model_a_predictions = data | RunInference(<model_handler_A>)
   model_b_predictions = model_a_predictions | beam.ParDo(post_processing()) | RunInference(<model_handler_B>)
```

The [Ensemble model using an image captioning and ranking example](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml/run_inference_multi_model.ipynb) notebook shows an end-to-end example of a cascade pipeline used to generate and rank image
captions. The solution consists of two open-source models:

1. **A caption generation model ([BLIP](https://github.com/salesforce/BLIP))** that generates
   candidate image captions from an input image.
2. **A caption ranking model ([CLIP](https://github.com/openai/CLIP))** that uses the image and
   candidate captions to rank the captions in the order in which they best describe the image.

