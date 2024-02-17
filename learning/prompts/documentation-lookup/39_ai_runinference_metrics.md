Prompt:
What metrics are available for monitoring the performance of ML model inference in Apache Beam?
Response:
Apache Beam lets you efficiently inference on ML models with the help of `RunInference API` which is supported in Python SDK starting from Apache Beam 2.40.0 and in Java SDK version 2.41.0 through Apache Beam’s [Multi-language Pipelines framework](https://beam.apache.org/documentation/programming-guide/#multi-language-pipelines).

RunInference API provides has built-in support for monitoring the performance of ML model inference. Following is a list of metrics commonly used for inference benchmarking:

- `model_byte_size`: size of the memory footprint of the model load and initialization
- `load_model_latency_milli_secs`: time taken to load and initialize the model
- `num_inferences`: total number of elements passed to run_inference()
- `inference_batch_latency_micro_secs`: time taken to perform inference across all batches of examples
- `inference_request_batch_byte_size`: size of the batch in bytes
- `inference_request_batch_size`: number of examples in the batch
- `failed_batches_counter`: number of failed batches

You can derive additoinal metrics from listed above as follows:
`Total time taken for inference` =` num_inferences` x `inference_batch_latency_micro_secs_MEAN`

Below is the simplistic example of how to use the `RunInference` API to inference on a LM model:

```python
  with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (
        pipeline
        | "Create inputs" >> beam.Create(<INPUTS>>)
        | "Tokenize" >> beam.ParDo(Tokenize(<TOKENIZER_NAME>))
        | "Inference" >> RunInference(<MODEL_HANDLER>))
```
After running the pipeline, you can query the metrics using the following code:

```python
metrics = pipeline.result.metrics().query(beam.metrics.MetricsFilter())
```
Metrics are also available in the Dataflow UI. For the whole source code of the example and instructions to run it in Google Dataflow, see [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/runinference_metrics/)

See here for a [](https://github.com/akvelon/beam/blob/371576a3b17b940380192378848dd00c55d0cc19/sdks/python/apache_beam/ml/inference/base.py#L1228)