---
title: "RunInference Metrics"
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

# RunInference Metrics Example

The main purpose of the example is to demonstrate and explain different metrics that are available when using the [RunInference](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/) transform to perform inference using a machine learning model. We use a pipeline that reads a list of sentences, tokeinzes the text, and uses a Transformer based model `distilbert-base-uncased-finetuned-sst-2-english` for classifying the texts into two different classes using `RunInference`.

We showcase different RunInference metrics when the pipeline is executed using the Dataflow Runner on CPU and GPU. The full example code can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/runinference_metrics/).


The file structure for entire pipeline is:

    runinference_metrics/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── transformations.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/transformations.py` contains the code for `beam.DoFn` and additional functions that are used for pipeline

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline

`config.py` defines some variables like GCP `PROJECT_ID`, `NUM_WORKERS` that are used multiple times

`setup.py` defines the packages/requirements for the pipeline to run

`main.py` contains the pipeline code and some additional functions used for running the pipeline


### How to Run the Pipeline
First, make sure you have installed the required packages. One should have access to a Google Cloud Project and then correctly configure the GCP variables like `PROJECT_ID`, `REGION`, and others in `config.py`. To use GPUs, follow the setup instructions [here](https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/dataflow/gpu-examples/pytorch-minimal).


1. Dataflow with CPU: `python main.py --mode cloud --device CPU`
2. Dataflow with GPU: `python main.py --mode cloud --device GPU`

The pipeline can be broken down into few simple steps:
1. Create a list of texts to use as an input using `beam.Create`
2. Tokenize the text
3. Use RunInference to do inference
4. Postprocess the output of RunInference

{{< highlight >}}
  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "Create inputs" >> beam.Create(inputs)
        | "Tokenize" >> beam.ParDo(Tokenize(cfg.TOKENIZER_NAME))
        | "Inference" >>
        RunInference(model_handler=KeyedModelHandler(model_handler))
        | "Decode Predictions" >> beam.ParDo(PostProcessor()))
{{< /highlight >}}


## RunInference Metrics

As mentioned above, we benchmarked the performance of RunInference using Dataflow on both CPU and GPU. These metrics can be seen in the GCP UI and can also be printed using

{{< highlight >}}
metrics = pipeline.result.metrics().query(beam.metrics.MetricsFilter())
{{< /highlight >}}


A snapshot of different metrics from GCP UI when using Dataflow on GPU:

  ![RunInference GPU metrics rendered on Dataflow](/images/runinference_metrics_snapshot.svg)

Some metrics commonly used for benchmarking are:

* `num_inferences`: represents the total number of elements passed to `run_inference()`.

* `inference_batch_latency_micro_secs_MEAN`: represents the average time taken to perform the inference across all batches of examples, measured in microseconds.

* `inference_request_batch_size_COUNT`: represents the total number of samples across all batches of examples (created from `beam.BatchElements`) to be passed to run_inference()

* `inference_request_batch_byte_size_MEAN`: represents the average size of all elements for all samples in all batches of examples (created from `beam.BatchElements`) to be passed to run_inference(). This is measured in bytes.

* `model_byte_size_MEAN`: represents the average memory consumed to load and initialize the model. This is measured in bytes.

* `load_model_latency_milli_secs_MEAN`: represents the average time taken to load and initialize the model. This is measured in milliseconds.

One can derive other relevant metrics like
* `Total time taken for inference` = `num_inferences x inference_batch_latency_micro_secs_MEAN`

