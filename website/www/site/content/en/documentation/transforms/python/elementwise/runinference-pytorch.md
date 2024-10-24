---
title: "RunInference with PyTorch"
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

# Use RunInference with PyTorch

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}
      </a>
   </td>
  </tr>
</table>

The following examples demonstrate how to create pipelines that use the Beam RunInference API and PyTorch.

## Example 1: PyTorch unkeyed model

In this example, we create a pipeline that uses a PyTorch RunInference transform on unkeyed data.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py"
  class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" torch_unkeyed_model_handler >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference_test.py" torch_unkeyed_model_handler >}}
{{< /highlight >}}

## Example 2: PyTorch keyed model

In this example, we create a pipeline that uses a PyTorch RunInference transform on keyed data.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py"
  class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" torch_keyed_model_handler >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference_test.py" torch_keyed_model_handler >}}
{{< /highlight >}}
