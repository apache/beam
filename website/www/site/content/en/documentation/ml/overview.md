---
title: "Overview"
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

# AI/ML pipelines

Beam <3 machine learning. Being productive and successful as a machine learning practitioner is often dependent on your ability to efficiently leverage large volumes of data in a way that is uniquely tailored to your resources, requirements, and budget. Whether starting your next AI/ML project or upscaling an existing project, consider adding Apache Beam to your project.

* Apache Beam enables you to process large volumes of data, both for preprocessing and for inference.
* It allows you to experiment with your data during the exploration phase of your project and provides a seamless transition when
  upscaling your data pipelines as part of your MLOps ecosystem in a production environment.
* It enables you to run your model in production on a varying data load, both in batch and streaming.

<iframe class="video video--medium-size" width="560" height="315" src="https://www.youtube.com/embed/ga2TNdrFRoU" frameborder="0" allowfullscreen></iframe>

## AI/ML workloads

Let’s take a look at the different building blocks that we need to create an end-to-end AI/ML use case and where Apache Beam can help.

![Overview of AI/ML building blocks and where Apache Beam can be used](/images/ml-workflows.svg)

1. Data ingestion: Incoming new data is stored in your file system or database, or it's published to a messaging queue.
2. **Data validation**: After you receieve your data, check the quality of your data. For example, you might want to detect outliers and calculate standard deviations and class distributions.
3. **Data preprocessing**: After you validate your data, transform the data so that it is ready to use to train your model.
4. Model training: When your data is ready, you can start training your AI/ML model. This step is typically repeated multiple times, depending on the quality of your trained model.
5. **Model validation**: Before you deploy your new model, validate its performance and accuracy.
6. **Model deployment**: Deploy your model, using it to run inference on new or existing data.

To keep your model up to date and performing well as your data grows and evolves, run these steps multiple times. In addition, you can apply MLOps to your project to automate the AI/ML workflows throughout the model and data lifecycle. Use orchestrators to automate this flow and to handle the transition between the different building blocks in your project.

You can use Apache Beam for data validation, data preprocessing, model validation, and model deployment/inference. The next section examines these building blocks in more detail and explores how they can be orchestrated.

## Data processing

You can use Apache Beam for data validation and preprocessing by setting up data pipelines that transform your data and output metrics computed from your data. Beam has a rich set of [I/O connectors](/documentation/io/built-in/) for ingesting and writing data, which allows you to integrate it with your existing file system, database, or messaging queue.

When developing your ML model, you can also first explore your data with the [Beam DataFrame API](/documentation/dsls/dataframes/overview/). The DataFrom API lets you identify and implement the required preprocessing steps, making it easier for you to move your pipeline to production.

Steps executed during preprocessing often also need to be applied before running inference, in which case you can use the same Beam implementation twice. Lastly, when you need to do postprocessing after running inference, Apache Beam allows you to incoporate the postprocessing into your model inference pipeline.

Further reading:
* [AI/ML pipelines in Beam: data processing](/documentation/ml/data-processing)

## Inference

Beam provides different ways to implement inference as part of your pipeline. You can run your ML model directly in your pipeline and apply it on big scale datasets, both in batch and streaming pipelines.

### RunInference

The recommended way to implement inference is by using the [RunInference API](/documentation/sdks/python-machine-learning/). RunInference takes advantage of existing Apache Beam concepts, such as the `BatchElements` transform and the `Shared` class, to enable you to use models in your pipelines to create transforms optimized for machine learning inferences. The ability to create arbitrarily complex workflow graphs also allows you to build multi-model pipelines.

You can integrate your model in your pipeline by using the corresponding model handlers. A `ModelHandler` is an object that wraps the underlying model and allows you to configure its parameters. Model handlers are available for PyTorch, scikit-learn, and TensorFlow. Examples of how to use RunInference for PyTorch, scikit-learn, and TensorFlow are shown in the [RunInference notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb).

Because they can process multiple computations simultaneously, GPUs are optimized for training artificial intelligence and deep learning models. RunInference also allows you to use GPUs for significant inference speedup. An example of how to use RunInference with GPUs is demonstrated on the [RunInference metrics](/documentation/ml/runinference-metrics) page.

Another usecase of running machine learning models is to run them on hardware devices. [Nvidia TensorRT](https://developer.nvidia.com/tensorrt) is a machine learning framework used to run inference on Nvidia hardware. See [TensorRT Inference](/documentation/ml/tensorrt-runinference) for an example of a pipeline that uses TensorRT and Beam with the RunInference transform and a BERT-based text classification model.

### Custom Inference

The RunInference API doesn't currently support making remote inference calls using, for example, the Natural Language API or the Cloud Vision API. Therefore, in order to use these remote APIs with Apache Beam, you need to write custom inference calls. The [Remote inference in Apache Beam notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/custom_remote_inference.ipynb) shows how to implement a custom remote inference call using `beam.DoFn`. When you implement a remote inference for real life projects, consider the following factors:

* API quotas and the heavy load you might incur on your external API. To optimize the calls to an external API, you can confgure `PipelineOptions` to limit the parallel calls to the external remote API.

* Be prepared to encounter, identify, and handle failure as gracefully as possible. Use techniques like exponential backoff and dead-letter queues (unprocessed messages queues).

* When running inference with an external API, batch your input together to allow for more efficient execution.

* Consider monitoring and measuring the performance of a pipeline when deploying, because monitoring can provide insight into the status and health of the application.

## Model validation

Model validation allows you to benchmark your model’s performance against a previously unseen dataset. You can extract chosen metrics, create visualizations, log metadata, and compare the performance of different models with the end goal of validating whether your model is ready to deploy. Beam provides support for running model evaluation on a TensorFlow model directly inside your pipeline.

Further reading:
* [ML model evaluation](/documentation/ml/model-evaluation): Illustrates how to integrate model evaluation as part of your pipeline by using [TensorFlow Model Analysis (TFMA)](https://www.tensorflow.org/tfx/guide/tfma).

## Orchestrators

In order to automate and track the AI/ML workflows throughout your project, you can use orchestrators such as [Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/) (KFP) or [TensorFlow Extended](https://www.tensorflow.org/tfx) (TFX). These orchestrators automate your different building blocks and handle the transitions between them.

When you use Apache Beam as one of the building blocks in your project, these orchestrators are able to launch your Apache Beam job and to keep track of the input and output of your pipeline. These tasks are essential when moving your AI/ML solution into production, because they allow you to handle your model and data over time and improve the quality and reproducibility of results.

Further reading:
* [ML Workflow Orchestration](/documentation/ml/orchestration): Illustrates how to orchestrate ML workflows consisting of multiple steps by using Kubeflow Pipelines and Tensorflow Extended.

## Examples

You can find examples of end-to-end AI/ML pipelines for several use cases:

* [Multi model pipelines in Beam](/documentation/ml/multi-model-pipelines): Explains how multi-model pipelines work and gives an overview of what you need to know to build one using the RunInference API.
* [Online Clustering in Beam](/documentation/ml/online-clustering): Demonstrates how to set up a real-time clustering pipeline that can read text from Pub/Sub, convert the text into an embedding using a transformer-based language model with the RunInference API, and cluster the text using BIRCH with stateful processing.
* [Anomaly Detection in Beam](/documentation/ml/anomaly-detection): Demonstrates how to set up an anomaly detection pipeline that reads text from Pub/Sub in real time and then detects anomalies using a trained HDBSCAN clustering model with the RunInference API.
* [Large Language Model Inference in Beam](/documentation/ml/large-language-modeling): Demonstrates a pipeline that uses RunInference to perform translation with the T5 language model which contains 11 billion parameters.
* [Per Entity Training in Beam](/documentation/ml/per-entity-training): Demonstrates a pipeline that trains a Decision Tree Classifier per education level for predicting if the salary of a person is >= 50k.
