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

Beam <3 Machine Learning. Being productive and successful as a machine learning practitioner is often dependent on your ability to efficiently leverage large volumes of data in a way that is uniquely tailored to your resources, requirements and budget. When starting your next AI/ML project or upscaling an existing one, a vital tool you should consider adding to your project is Beam.

Beam enables you to process large volumes of data, both for preprocessing and for inference. It allows you to experiment with your data during the exploration phase of your project, while providing a seamless transition to upscaling your data pipelines as part of your MLOps ecosystem in a production environment. It allows you to run your model in production on a varying data load, both in batch and streaming.

## AI/ML workloads

Let’s take a look at the different building blocks that we need to build an end-to-end AI/ML use case, and where Beam will come in handy in building those blocks.

![Overview of  AI/ML building blocks & where Beam can be used](/images/ml-workflows.svg)

1. Data ingestion: incoming new data will be stored in your filesystem, database or published on a messaging queue.
2. **Data validation**: once you have received your data you need to check the quality of your data such as detecting outliers and reporting on standard deviations and class distributions.
3. **Data preprocessing**: after validating your data, you need to transform it so that it is ready to be used for training your model.
4. Model training: once your data is ready, you can start training your AI/ML model. This step will typically be repeated multiple times depending on the quality of your trained model.
5. Model validation: before deploying your new model you need to validate its performance and accuracy.
6. **Model deployment**: finally you can deploy your model, meaning it can run inference on any new or existing data.

All of these steps can be executed multiple times, as your data might grow and evolve over time and you want your model to stay up to date and guarantee its best performance. This is why it is very important to apply MLOps to your project, meaning that you aim to automate the AI/ML workflows throughout the model and data lifecycle. This can be achieved by using orchestrators that automate this flow and handle the transition between the different building blocks in your project.

Beam can be used for data validation, data preprocessing and model deployment/inference. We will now take a look at these different building blocks in more detail and at how they can be orchestrated. Finally, you can also find full examples of AI/ML pipelines in Beam.

## Data processing

Data validation and preprocessing can be done in Beam by setting up data pipelines that transform your data and output metrics computed from your data. Beam has a rich set of [IO connectors](https://beam.apache.org/documentation/io/built-in/) for ingesting and writing data, which means you can easily integrate it with your existing filesystem, database or messaging queue. When developing your ML model, you can also first explore your data with the [Beam DataFrame API](https://beam.apache.org/documentation/dsls/dataframes/overview/) so that you can identify and implement the required preprocessing steps allowing you to iterate faster towards production. Another common pattern is that the steps executed during preprocessing need to also be applied before running inference, in which case you can use the same Beam implementation twice. Lastly, if you need to do post-processing after running inference, this can also be done as part of your model inference pipeline.

Further reading:
* [AI/ML pipelines in Beam: data processing](/documentation/ml/data-processing)

## Inference

Beam provides different ways of implementing inference as part of your pipeline. This way you can run your ML model directly in your pipeline and apply it on big scale datasets, both in batch and streaming pipelines.

### RunInference
The recommended way to implement inference is by using the [RunInference API](https://beam.apache.org/documentation/sdks/python-machine-learning/). RunInference takes advantage of existing Apache Beam concepts, such as the `BatchElements` transform and the `Shared` class, to enable you to use models in your pipelines to create transforms optimized for machine learning inferences. The ability to create arbitrarily complex workflow graphs also allows you to build multi-model pipelines.

You can easily integrate your model in your pipeline by using the corresponding model handlers. A `ModelHandler` is an object that wraps the underlying model and allows you to configure its parameters. Model handlers are available for PyTorch, Scikit-learn and TensorFlow. Examples of how to use RunInference for PyTorch, Scikit-learn and TensorFlow are shown in this [notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_pytorch_tensorflow_sklearn.ipynb).

GPUs are optimized for training artificial intelligence and deep learning models as they can process multiple computations simultaneously. RunInference also allows you to use GPUs for significant inference speedup. An example of how to use RunInference with GPUs is demonstrated [here](/documentation/ml/runinference-metrics).

### Custom Inference
As of now, RunInference API doesn't support making remote inference calls (e.g. Natural Language API, Cloud Vision API and others). Therefore, in order to use these remote APIs with Beam, one needs to write custom inference call. The [notebook](https://github.com/apache/beam/blob/master/examples/notebooks/beam-ml/custom_remote_inference.ipynb) shows how you can implement such a custom remote inference call using `beam.DoFn`. While implementing such a remote inference for real life projects, you need to think about following:

* API quotas and the heavy load you might incur on your external API. For optimizing the calls to external API, you can confgure `PipelineOptions` to limit the parallel calls to the external remote API.

* You must be prepared to encounter, identify, and handle failure as gracefully as possible. We recommend using techniques like `Exponential backoff` and `Dead letter queues`.

* When running inference with an external API, you should batch your input together to allow for more efficient execution.

* You should consider monitoring and measuring performance of a pipeline when deploying since monitoring can provide insight into the status and health of the application.


## Orchestrators

In order to automate and track the AI/ML workflows throughout your project, you can use orchestrators such as [Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/) (KFP) or [TensorFlow Extended](https://www.tensorflow.org/tfx) (TFX) that automate your different building blocks and handle the transitions between them. When you use Beam as one of the building blocks in your project, these orchestrators will then be able to launch your Beam job and keep track of the input and output of your pipeline. This is essential when moving your AI/ML solution into production as it allows you to handle your model and data over time and to guarantee good results and reproducibility.

## Examples

You can find examples of end-to-end AI/ML pipelines for several use cases:
* [ML Workflow Orchestration](/documentation/ml/orchestration): illustrates how ML workflows consisting of multiple steps can be orchestrated by using Kubeflow Pipelines and Tensorflow Extended.
* [Multi model pipelines in Beam](/documentation/ml/multi-model-pipelines): explains how multi-model pipelines work and gives an overview of what you need to know to build one using the RunInference API.
* [Online Clustering in Beam](/documentation/ml/online-clustering): demonstrates how to setup a realtime clustering pipeline that can read text from PubSub, convert the text into an embedding using a transformer based language model with the RunInference API, and cluster them using BIRCH with Stateful Processing.
* [Anomaly Detection in Beam](/documentation/ml/anomaly-detection): demonstrates how to setup an anomaly detection pipeline that reads text from PubSub in real-time, and then detects anomaly using a trained HDBSCAN clustering model with the RunInference API.