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

## Inference

There are several ways to use and deploy your model:
1. Making it available for online predictions via an API
2. Running it in real-time as new data becomes available in a pipeline
3. Running it in batch on an existing dataset

Beam is ideally suitable for the last 2 use cases. In this case your data will run through a pipeline (streaming or batch), and you can obtain predictions by running inference in one of the steps of your pipeline. Beam provides the [RunInference API](https://beam.apache.org/documentation/sdks/python-machine-learning/) to facilitate the integration of your model into a pipeline step. When running your model, a common requirement is to enable GPU execution. Beam also provides support for this.

## Orchestrators

In order to automate and track the AI/ML workflows throughout your project, you can use orchestrators such as [Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/) (KFP) or [TensorFlow Extended](https://www.tensorflow.org/tfx) (TFX) that automate your different building blocks and handle the transitions between them. When you use Beam as one of the building blocks in your project, these orchestrators will then be able to launch your Beam job and keep track of the input and output of your pipeline. This is essential when moving your AI/ML solution into production as it allows you to handle your model and data over time and to guarantee good results and reproducibility.

## Examples

You can find examples of end-to-end AI/ML pipelines for several use cases:
* [Multi model pipelines in Beam](/documentation/ml/multi-model-pipelines)
* [Online Clustering in Beam](/documentation/ml/online-clustering)
