---
title: "RunInference"
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

# RunInference

{{< localstorage language language-py >}}

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.ml.inference.base" class="RunInference" >}}
      </a>
   </td>
  </tr>
</table>

Uses models to do local and remote inference. A `RunInference` transform performs inference on a `PCollection` of examples using a machine learning (ML) model. The transform outputs a `PCollection` that contains the input examples and output predictions. Avaliable in Apache Beam 2.40.0 and later versions.

For more information about Beam RunInference APIs, see the [About Beam ML](https://beam.apache.org/documentation/ml/about-ml) page and the [RunInference API pipeline](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference) examples.

## Examples

The following examples show how to create pipelines that use the Beam RunInference API to make predictions based on models.

{{< table >}}
| Framework | Example |
| ----- | ----- |
| PyTorch | [PyTorch unkeyed model](/documentation/transforms/python/elementwise/runinference-pytorch/#example-1-pytorch-unkeyed-model) |
| PyTorch | [PyTorch keyed model](/documentation/transforms/python/elementwise/runinference-pytorch/#example-2-pytorch-keyed-model) |
| Sklearn| [Sklearn unkeyed model](/documentation/transforms/python/elementwise/runinference-sklearn/#example-1-sklearn-unkeyed-model) |
| Sklearn | [Sklearn keyed model](/documentation/transforms/python/elementwise/runinference-sklearn/#example-2-sklearn-keyed-model) |:
{{< /table >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}
