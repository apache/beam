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

{{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}

Uses models to do local and remote inference. A `RunInference` transform performs inference on a `PCollection` of examples using a machine learning (ML) model. The transform outputs a `PCollection` that contains the input examples and output predictions.

You must have Apache Beam 2.40.0 or later installed to run these pipelines.

See more [RunInference API pipeline examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference).

## Examples

In the following examples, we explore how to create pipelines that use the Beam RunInference API to make predictions based on models.

### Example 1: PyTorch unkeyed model

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

### Example 2: PyTorch keyed model

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

### Example 3: Sklearn unkeyed model

In this example, we create a pipeline that uses an SKlearn RunInference transform on unkeyed data.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py"
  class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" sklearn_unkeyed_model_handler >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference_test.py" sklearn_unkeyed_model_handler >}}
{{< /highlight >}}

### Example 4: Sklearn keyed model

In this example, we create a pipeline that uses an SKlearn RunInference transform on keyed data.

{{< highlight language="py" file="sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py"
  class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference.py" sklearn_keyed_model_handler >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/runinference_test.py" sklearn_keyed_model_handler >}}
{{< /highlight >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.ml.inference" class="RunInference" >}}
