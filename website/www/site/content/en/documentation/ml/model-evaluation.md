---
title: "ML Model Evaluation"
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

# ML Model Evaluation

Model evaluation is an essential part of your ML journey. It allows you to benchmark your model’s performance against an unseen dataset. You can extract chosen metrics, create visualizations, log metadata, and compare the performance of different models. In your MLOps ecosystem, a model evaluation step is crucial for monitoring the evolution of your model or multiple models when your dataset grows or changes over time and when you retrain your model.

Beam provides support for running model evaluation on a TensorFlow model directly inside your pipeline by using a PTransform called [ExtractEvaluateAndWriteResults](https://www.tensorflow.org/tfx/model_analysis/api_docs/python/tfma/ExtractEvaluateAndWriteResults). This PTransform is part of [TensorFlow Model Analysis (TFMA)](https://www.tensorflow.org/tfx/guide/tfma), a library for performing model evaluation across different slices of data. TFMA performs its computations in a distributed manner over large amounts of data using Beam, allowing you to evaluate models on large amounts of data in a distributed manner. These metrics are compared over slices of data and visualized in Jupyter or Colab notebooks.

## TFMA Example

Here is an example of how you can use ExtractEvaluateAndWriteResults to evaluate a linear regression model.

First, define the configuration to specify the model information, the chosen metrics, and optionally the data slices.

```python
from google.protobuf import text_format

# Define the TFMA evaluation configuration
eval_config = text_format.Parse("""

## Model information

  model_specs {
    # For keras and serving models, you need to add a `label_key`.
    label_key: "output"
  }

## This post-training metric information is merged with any built-in

## metrics from training

  metrics_specs {
    metrics { class_name: "ExampleCount" }
    metrics { class_name: "MeanAbsoluteError" }
    metrics { class_name: "MeanSquaredError" }
    metrics { class_name: "MeanPrediction" }
  }

  slicing_specs {}
""", tfma.EvalConfig())
```

Then, create a pipeline to run the evaluation:

```python
from tfx_bsl.public import tfxio

eval_shared_model = tfma.default_eval_shared_model(
    eval_saved_model_path='model_path', eval_config=eval_config)

tfx_io = tfxio.TFExampleRecord(
          file_pattern='tfrecords_path',
          raw_record_column_name=tfma.ARROW_INPUT_COLUMN)

# Run evaluation
with beam.Pipeline() as pipeline:
    _ = (
        pipeline
        | 'ReadData' >> tfx_io.BeamSource()
        | 'EvalModel' >> tfma.ExtractEvaluateAndWriteResults(
           eval_shared_model=eval_shared_model,
           eval_config=eval_config,
           output_path='output_path'))
```

This pipeline saves the results, including the config file, metrics, plots, and so on, to a chosen output_path.

## TFMA End-to-end Example

For a full end-to-end example of model evaluation in TFMA on Beam, see the [tfma_beam notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/tfma_beam.ipynb).

This example shows the creation of tfrecords from an open source dataset, the training of a model, and the evaluation in Beam.
