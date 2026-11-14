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

RunInference reports the following metrics:

{{< table >}}
| Metric | Type | Description |
| ------ | ---- | ----------- |
| `num_inferences` | Counter | The total number of elements passed to `run_inference()`. Elements in batches where inference failed aren't counted. |
| `failed_batches_counter` | Counter | The number of batches for which inference raised an exception. |
| `inference_request_batch_size` | Distribution | The number of elements in each batch of examples (created from `beam.BatchElements`) passed to `run_inference()`. |
| `inference_request_batch_byte_size` | Distribution | The size of each batch of examples, in bytes, as computed by the model handler's `get_num_bytes()` method. |
| `inference_batch_latency_micro_secs` | Distribution | The time taken to perform inference on each batch of examples, measured in microseconds. |
| `load_model_latency_milli_secs` | Distribution | The time taken to load and initialize the model, measured in milliseconds. |
| `model_byte_size` | Distribution | The memory consumed to load and initialize the model, in bytes. RunInference measures it as the increase in the worker process's peak memory usage while the model loads. On platforms where the Python `resource` module isn't available, such as Windows, this metric is always 0. |
{{< /table >}}

Counters report a total, and distributions record one value at a time. For more information, see [Types of metrics](/documentation/programming-guide/#types-of-metrics). In the Google Cloud console, each distribution appears as four metrics with the suffixes `_COUNT`, `_MIN`, `_MAX`, and `_MEAN`, where `_COUNT` is the number of values recorded.

`inference_request_batch_size`, `inference_request_batch_byte_size`, and `inference_batch_latency_micro_secs` record one value for each batch where inference succeeded, so their `_COUNT` is the number of batches, not the number of elements. For example, in the preceding image, `inference_request_batch_size_COUNT` is 4,013, the number of batches, while `num_inferences` is 6,000, the number of elements. `load_model_latency_milli_secs` and `model_byte_size` record one value each time the model is loaded.

RunInference reports these metrics in the namespace returned by the model handler's `get_metrics_namespace()` method, such as `BeamML_PyTorch` for the PyTorch model handlers. To use a different namespace, pass the `metrics_namespace` argument to `RunInference`.

You can also derive other relevant metrics, such as in the following example.
* `Total time taken for inference` = `inference_batch_latency_micro_secs_COUNT x inference_batch_latency_micro_secs_MEAN`

