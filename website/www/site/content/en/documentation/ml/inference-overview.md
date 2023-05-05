---
title: "Prediction and inference overview"
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

# Prediction and inference


Beam provides different ways to implement inference as part of your pipeline. You can run your ML model directly in your pipeline and apply it on big scale datasets, both in batch and streaming pipelines.

## Use the RunInference API

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

The RunInference API is available with the Beam Python SDK versions 2.40.0 and later. You can use Apache Beam with the RunInference API to use machine learning (ML) models to do local and remote inference with batch and streaming pipelines. Starting with Apache Beam 2.40.0, PyTorch and Scikit-learn frameworks are supported. Tensorflow models are supported through `tfx-bsl`. For more deatils about using RunInference with Python, see [Machine Learning with Python](/documentation/sdks/python-machine-learning/).

The RunInference API is available with the Beam Java SDK versions 2.41.0 and later through Apache Beam's [Multi-language Pipelines framework](/documentation/programming-guide/#multi-language-pipelines). For information about the Java wrapper transform, see [RunInference.java](https://github.com/apache/beam/blob/master/sdks/java/extensions/python/src/main/java/org/apache/beam/sdk/extensions/python/transforms/RunInference.java). To try it out, see the [Java Sklearn Mnist Classification example](https://github.com/apache/beam/tree/master/examples/multi-language).

You can create multiple types of transforms using the RunInference API: the API takes multiple types of setup parameters from model handlers, and the parameter type determines the model implementation.

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to use the RunInference transform | [Modify a Python pipeline to use an ML model](/documentation/sdks/python-machine-learning/#modify-a-python-pipeline-to-use-an-ml-model) |
| I want to use RunInference with PyTorch | [Use RunInference with PyTorch](/documentation/transforms/python/elementwise/runinference-pytorch/) |
| I want to use RunInference with Sklearn | [Use RunInference with Sklearn](/documentation/transforms/python/elementwise/runinference-sklearn/) |
| I want to use pre-trained models (PyTorch, Scikit-learn, or TensorFlow) | [Use pre-trained models](/documentation/ml/about-ml/#use-pre-trained-models) |:
{{< /table >}}

## Use cases

{{< table >}}
| Task | Example |
| ------- | ---------------|
| I want to build a pipeline with multiple models | [Multi-Model Pipelines](/documentation/ml/multi-model-pipelines) |
| I want to build a custom model handler with TensorRT | [Use TensorRT with RunInference](/documentation/ml/tensorrt-runinference) |
| I want to use LLM inference | [Large Language Model Inference](/documentation/ml/large-language-modeling/) |:
{{< /table >}}
