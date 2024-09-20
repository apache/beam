Prompt:
What are the AI and ML capabilities in Apache Beam?

Response:
Apache Beam has several built-in [AI and ML capabilities](https://beam.apache.org/documentation/ml/overview/) that enable you to:
- Process large datasets for both preprocessing and model inference.
- Conduct exploratory data analysis and smoothly scale up data pipelines in production as part of your MLOps ecosystem.
- Run your models in production with varying data loads, both in batch and streaming pipelines.

For common AI platform integration patterns in Apache Beam, see [AI Platform integration patterns](https://beam.apache.org/documentation/patterns/ai-platform/).

The recommended way to implement inference in Apache Beam is by using the [RunInference API](https://beam.apache.org/documentation/sdks/python-machine-learning/). For more information about how to use RunInference for PyTorch, scikit-learn, and TensorFlow, see the [Use RunInference in Apache Beam](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb) example in GitHub.

Using pre-trained models in Apache Beam is also supported with [PyTorch](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch.ipynb), [scikit-learn](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_sklearn.ipynb), and [TensorFlow](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb). Running inference on  [custom models](https://beam.apache.org/documentation/ml/about-ml/#use-custom-models) is also supported.

Apache Beam also supports automatically updating the model being used with the `RunInference PTransform` in streaming pipelines without stopping the pipeline. The feature lets you avoid downtime downtime. For more information, see [Automatic model refresh](https://beam.apache.org/documentation/ml/about-ml/#automatic-model-refresh).
For more information about using machine learning models with Apache Beam, see [Running ML models now easier with new Dataflow ML innovations on Apache Beam](https://cloud.google.com/blog/products/ai-machine-learning/dataflow-ml-innovations-on-apache-beam/).

For an example that uses the Apache Beam ML integration, see [BigQuery ML integration](https://beam.apache.org/documentation/patterns/bqml/).
