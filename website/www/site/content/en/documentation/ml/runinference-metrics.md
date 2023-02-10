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

# RunInference Metrics

This example demonstrates and explains different metrics that are available when using the [RunInference](/documentation/transforms/python/elementwise/runinference/) transform to perform inference using a machine learning model. The example uses a pipeline that reads a list of sentences, tokenizes the text, and uses the transformer-based model `distilbert-base-uncased-finetuned-sst-2-english` with `RunInference` to classify the pieces of text into two classes.

When you run the pipeline with the Dataflow runner, different RunInference metrics are available with CPU and with GPU. This example demonstrates both types of metrics.

- You can find the full code for this example on [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/runinference_metrics/).
- You can see RunInference benchmarks on the [Performance test metrics](http://s.apache.org/beam-community-metrics/d/ZpS8Uf44z/python-ml-runinference-benchmarks?orgId=1) page.


The following diagram shows the file structure for the entire pipeline.

    runinference_metrics/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── transformations.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/transformations.py` contains the code for `beam.DoFn` and additional functions that are used for the pipeline.

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline.

`config.py` defines variables that are used multiple times, like the Google Cloud `PROJECT_ID` and `NUM_WORKERS`.

`setup.py` defines the packages and requirements for the pipeline to run.

`main.py` contains the pipeline code and additional functions used for running the pipeline.


## Run the Pipeline

Install the required packages. For this example, you need access to a Google Cloud project, and you need to configure the Google Cloud variables, like `PROJECT_ID`, `REGION`, and others, in the `config.py` file. To use GPUs, follow the setup instructions in the [PyTorch GPU minimal pipeline](https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/dataflow/gpu-examples/pytorch-minimal) example on GitHub.


1. Dataflow with CPU: `python main.py --mode cloud --device CPU`
2. Dataflow with GPU: `python main.py --mode cloud --device GPU`

The pipeline includes the following steps:
1. Create a list of texts to use as an input using `beam.Create`.
2. Tokenize the text.
3. Use RunInference to do inference.
4. Postprocess the output of RunInference.

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

As mentioned previously, we benchmarked the performance of RunInference using Dataflow on both CPU and GPU. You can see these metrics in the Google Cloud console, or you can use the following line to print the metrics:

{{< highlight >}}
metrics = pipeline.result.metrics().query(beam.metrics.MetricsFilter())
{{< /highlight >}}


The following image shows a snapshot of different metrics in the Google Cloud console when using Dataflow on GPU:

  ![RunInference GPU metrics rendered on Dataflow](/images/runinference_metrics_snapshot.svg)

Some metrics commonly used for benchmarking are:

* `num_inferences`: Represents the total number of elements passed to `run_inference()`.

* `inference_batch_latency_micro_secs_MEAN`: Represents the average time taken to perform inference across all batches of examples, measured in microseconds.

* `inference_request_batch_size_COUNT`: Represents the total number of samples across all batches of examples (created from `beam.BatchElements`) to be passed to `run_inference()`.

* `inference_request_batch_byte_size_MEAN`: Represents the average size of all elements for all samples in all batches of examples (created from `beam.BatchElements`) to be passed to `run_inference()`. This metric is measured in bytes.

* `model_byte_size_MEAN`: Represents the average memory consumed to load and initialize the model, measured in bytes.

* `load_model_latency_milli_secs_MEAN`: Represents the average time taken to load and initialize the model, measured in milliseconds.

You can also derive other relevant metrics, such as in the following example.
* `Total time taken for inference` = `num_inferences x inference_batch_latency_micro_secs_MEAN`

