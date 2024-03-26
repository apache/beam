---
title: "Get Started with ML"
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

# Get started with AI/ML pipelines

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}
      </a>
   </td>
   <td>
      <a target="_blank" class="button"
          href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/extensions/python/transforms/RunInference.html">
        <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="30px"
            alt="Javadoc" />
      Javadoc
      </a>
    </td>
  </tr>
</table>

Being productive and successful as a machine learning practitioner is often dependent on your ability to efficiently leverage large volumes of data in a way that is uniquely tailored to your resources, requirements, and budget. Whether starting your next AI/ML project or upscaling an existing project, consider adding Apache Beam to your project.

## Use Beam ML

I want use Beam ML to do:

* [Prediction and inference](#prediction-and-inference)
* [Data processing](#data-processing)
* [Workflow orchestration](#workflow-orchestration)
* [Model training](#model-training)
* [Anomaly detection](#use-cases)


## Prediction and inference

Beam provides different ways to implement inference as part of your pipeline. You can run your ML model directly in your pipeline and apply it on big scale datasets, both in batch and streaming pipelines.

### RunInference

The RunInfernce API is available with the Beam Python SDK versions 2.40.0 and later. You can use Apache Beam with the RunInference API to use machine learning (ML) models to do local and remote inference with batch and streaming pipelines. Starting with Apache Beam 2.40.0, PyTorch and Scikit-learn frameworks are supported. Tensorflow models are supported through `tfx-bsl`. For more deatils about using RunInference, see [About Beam ML](/documentation/ml/about-ml).

The RunInference API is available with the Beam Java SDK versions 2.41.0 and later through Apache Beam's [Multi-language Pipelines framework](/documentation/programming-guide/#multi-language-pipelines). For information about the Java wrapper transform, see [RunInference.java](https://github.com/apache/beam/blob/master/sdks/java/extensions/python/src/main/java/org/apache/beam/sdk/extensions/python/transforms/RunInference.java). To try it out, see the [Java Sklearn Mnist Classification example](https://github.com/apache/beam/tree/master/examples/multi-language).

You can create multiple types of transforms using the RunInference API: the API takes multiple types of setup parameters from model handlers, and the parameter type determines the model implementation.

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to use the RunInference transform | [Modify a Python pipeline to use an ML model](/documentation/ml/about-ml/#modify-a-python-pipeline-to-use-an-ml-model) |
| I want to use RunInference with PyTorch | [Use RunInference with PyTorch](/documentation/transforms/python/elementwise/runinference-pytorch/) |
| I want to use RunInference with Sklearn | [Use RunInference with Sklearn](/documentation/transforms/python/elementwise/runinference-sklearn/) |
| I want to use pre-trained models (PyTorch, Scikit-learn, or TensorFlow) | [Use pre-trained models](/documentation/ml/about-ml/#use-pre-trained-models) |
| I want to update my model in production | [Use WatchFilePattern to auto-update ML models in RunInference](/documentation/ml/side-input-updates/) |:
{{< /table >}}


### Prediction and inference examples

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to build a pipeline with multiple models | [Multi-Model Pipelines](/documentation/ml/multi-model-pipelines) |
| I want to build a custom model handler with TensorRT | [Use TensorRT with RunInference](/documentation/ml/tensorrt-runinference) |
| I want to use LLM inference | [Large Language Model Inference](/documentation/ml/large-language-modeling/) |
| I want to build a multi-language inference pipeline | [Using RunInference from Java SDK](/documentation/ml/multi-language-inference/) |:
{{< /table >}}

## Data processing

You can use Apache Beam for data validation and preprocessing by setting up data pipelines that transform your data and output metrics computed from your data. Beam has a rich set of [I/O connectors](/documentation/io/built-in/) for ingesting and writing data, which allows you to integrate it with your existing file system, database, or messaging queue.

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to transform my data for preprocessing| [Preprocess data with MLTransform](/documentation/ml/preprocess-data) |
| I want to explore my data | [Data exploration workflow and example](/documentation/ml/data-processing) |
| I want to enrich my data | [Data enrichment wth Enrichment transform](https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/beam-ml/bigtable_enrichment_transform.ipynb) |:
{{< /table >}}


## Workflow orchestration

In order to automate and track the AI/ML workflows throughout your project, you can use orchestrators such as [Kubeflow pipelines](https://www.kubeflow.org/docs/components/pipelines/introduction/) (KFP) or [TensorFlow Extended](https://www.tensorflow.org/tfx) (TFX). These orchestrators automate your different building blocks and handle the transitions between them.

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to use ML-OPS workflow orchestrators | [Workflow Orchestration](/documentation/ml/orchestration/) |
| I want to use Beam with TensorFlow Extended (TFX) | [Tensorflow Extended (TFX)](/documentation/ml/orchestration/#tensorflow-extended-tfx) |
| I want to use Beam with Kubeflow | [Kubeflow pipelines (KFP)](/documentation/ml/orchestration/#kubeflow-pipelines-kfp) |:
{{< /table >}}

When you use Apache Beam as one of the building blocks in your project, these orchestrators are able to launch your Apache Beam job and to keep track of the input and output of your pipeline. These tasks are essential when moving your AI/ML solution into production, because they allow you to handle your model and data over time and improve the quality and reproducibility of results.

## Model training

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to use per-entity training | [Per Entity Training](/documentation/ml/per-entity-training) |
| I want to cluster text | [Online Clustering Example](/documentation/ml/online-clustering) |
| I want to benchmark model performance | [ML Model Evaluation](/documentation/ml/model-evaluation/) |:
{{< /table >}}

## Use cases

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to build an anomaly detection pipeline | [Anomaly Detection Example](/documentation/ml/anomaly-detection/) |:
{{< /table >}}

## Reference

* [RunInference metrics](/documentation/ml/runinference-metrics/)
* [ML model evaluation](/documentation/ml/model-evaluation/)
* [RunInference public codelab](https://colab.sandbox.google.com/github/apache/beam/blob/master/examples/notebooks/beam-ml/run_inference_basic.ipynb)
* [RunInference notebooks](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml)
