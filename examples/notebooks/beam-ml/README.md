<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# ML Sample Notebooks

Starting with the Apache Beam SDK version 2.40, users have access to a
[RunInference](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.RunInference)
transform.

This transform allows you to make predictions and inference on data with machine learning (ML) models.
The model handler abstracts the user from the configuration needed for
specific frameworks, such as Tensorflow, PyTorch, and others. For a full list of supported frameworks,
see the Apache Beam [Machine Learning](https://beam.apache.org/documentation/sdks/python-machine-learning) page.

## Using The Notebooks

These notebooks illustrate ways to use Apache Beam's RunInference transforms, as well as different
use cases for [ModelHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.ModelHandler) implementations.
Beam comes with [multiple ModelHandler implementations](https://beam.apache.org/documentation/sdks/python-machine-learning/#modify-a-pipeline-to-use-an-ml-model).

### Loading the Notebooks

1. To get started quickly with notebooks, use [Colab](https://colab.sandbox.google.com/).
2. In Colab, open the notebook from GitHub using the notebook URL, for example:
```
https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb
```

3. To run most notebooks, you need to change the Google Cloud project and bucket
to your project and bucket.

## Notebooks

This section contains the following example notebooks.

### Prediction and inference with pretrained models

* [Apache Beam RunInference for PyTorch](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch.ipynb)
* [Apache Beam RunInference for scikit-learn](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_sklearn.ipynb)
* [Apache Beam RunInference with TensorFlow](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb)
* [Use RunInference with a model from TensorFlow Hub](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_with_tensorflow_hub.ipynb)
* [Apache Beam RunInference with XGBoost](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_xgboost.ipynb)
* [Use RunInference with TFX](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow_with_tfx.ipynb)
* [Use RunInference in Apache Beam](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb)

### Custom inference

* [Bring your own machine learning (ML) model to Apache Beam RunInference](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_custom_inference.ipynb)
* [Remote inference](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/custom_remote_inference.ipynb)

### Automatic Model Refresh

* [Update your model in production](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/automatic_model_refresh.ipynb)

### Multi-model pipelines

* [Ensemble model using an image captioning and ranking](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_multi_model.ipynb)

### Model Evaluation

* [Use TFMA to evaluate and compare model performance](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/tfma_beam.ipynb)

### Data processing

* [Preprocessing with the Apache Beam DataFrames API](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/dataframe_api_preprocessing.ipynb)
