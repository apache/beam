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
# ML sample notebooks

Starting with the Apache Beam SDK version 2.40, users have access to a
[RunInference](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.RunInference)
transform.

This transform allows you to make predictions and inference on data with machine learning (ML) models.
The model handler abstracts the user from the configuration needed for
specific frameworks, such as Tensorflow, PyTorch, and others. For a full list of supported frameworks,
see the [About Beam ML](https://beam.apache.org/documentation/ml/about-ml/) page.

## Use the notebooks

These notebooks illustrate ways to use Apache Beam's RunInference transforms, as well as different
use cases for [`ModelHandler`](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.ModelHandler) implementations.
Beam comes with [multiple `ModelHandler` implementations](https://beam.apache.org/documentation/ml/about-ml/#modify-a-python-pipeline-to-use-an-ml-model).

### Load the notebooks

1. To get started quickly with notebooks, use [Colab](https://colab.sandbox.google.com/).
2. In Colab, open the notebook from GitHub using the notebook URL, for example:
```
https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb
```

3. To run most notebooks, you need to change the Google Cloud project and bucket
to your project and bucket.

## Notebooks

This section contains the following example notebooks.

### Data processing

* [Generate text embeddings by using the Vertex AI API](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/data_preprocessing/vertex_ai_text_embeddings.ipynb)
* [Generate text embeddings by using Hugging Face Hub models](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/data_preprocessing/huggingface_text_embeddings.ipynb)
* [Compute and apply vocabulary on a dataset](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/data_preprocessing/compute_and_apply_vocab.ipynb)
* [Use MLTransform to scale data](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/data_preprocessing/scale_data.ipynb)
* [Preprocessing with the Apache Beam DataFrames API](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/dataframe_api_preprocessing.ipynb)

### Data enrichment

* [Use Bigtable to enrich data](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/bigtable_enrichment_transform.ipynb)
* [Use Vertex AI Feature Store for feature enrichment](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/vertex_ai_feature_store_enrichment.ipynb)

### Prediction and inference with pretrained models

* [Apache Beam RunInference for PyTorch](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch.ipynb)
* [Apache Beam RunInference for scikit-learn](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_sklearn.ipynb)
* [Apache Beam RunInference with TensorFlow](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow.ipynb)
* [Use RunInference with a model from TensorFlow Hub](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_with_tensorflow_hub.ipynb)
* [Apache Beam RunInference with Hugging Face](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_huggingface.ipynb)
* [Apache Beam RunInference with XGBoost](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_xgboost.ipynb)
* [Use RunInference with TFX](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_tensorflow_with_tfx.ipynb)
* [Use RunInference with a remotely deployed Vertex AI endpoint](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_vertex_ai.ipynb)
* [Use RunInference in Apache Beam](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb)
* [Use RunInference with a LLM](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_generative_ai.ipynb)
* [Use RunInference with Beam's windowing semantics](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_windowing.ipynb)

### Custom inference

* [Bring your own machine learning (ML) model to Apache Beam RunInference](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_custom_inference.ipynb)
* [Remote inference](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/custom_remote_inference.ipynb)

### Machine Learning Use Cases

* [Image processing with Apache Beam](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/image_processing_tensorflow.ipynb)
* [Natural language processing with Apache Beam](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/nlp_tensorflow_streaming.ipynb)
* [Speech emotion recognition with Apache Beam](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/speech_emotion_tensorflow.ipynb)

### Automatic Model Refresh

* [Update your model in production](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/automatic_model_refresh.ipynb)

### Multi-model pipelines

* [Ensemble model using an image captioning and ranking](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_multi_model.ipynb)
* [Run ML inference with multiple differently-trained models](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/per_key_models.ipynb)

### Model Evaluation

* [Use TFMA to evaluate and compare model performance](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/tfma_beam.ipynb)
