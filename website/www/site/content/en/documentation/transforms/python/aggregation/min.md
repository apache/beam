---
title: "Min"
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

# Min

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="CombineGlobally" >}}

Gets the element with the minimum value within each aggregation.

## Examples

In the following example, we create a pipeline with a `PCollection`.
Then, we get the element with the minimum value in different ways.

### Example 1: Minimum element in a PCollection

We use `Combine.Globally()` to get the minimum element from the *entire* `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/min.py" min_globally >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/min_test.py" min_element >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/min.py" >}}

### Example 2: Minimum elements for each key

We use `Combine.PerKey()` to get the minimum element for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/min.py" min_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/min_test.py" elements_with_min_value_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/min.py" >}}

## Related transforms

* [CombineGlobally](/documentation/transforms/python/aggregation/combineglobally)
* [CombinePerKey](/documentation/transforms/python/aggregation/combineperkey)
* [Max](/documentation/transforms/python/aggregation/max)
* [Mean](/documentation/transforms/python/aggregation/mean)
* [Sum](/documentation/transforms/python/aggregation/sum)

{{< button-pydoc path="apache_beam.transforms.core" class="CombineGlobally" >}}
