---
title: "Mean"
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

# Mean

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.combiners" class="Mean" >}}

Transforms for computing the arithmetic mean of the elements in a collection,
or the mean of the values associated with each key in a collection of
key-value pairs.

## Examples

In the following example, we create a pipeline with a `PCollection`.
Then, we get the element with the average value in different ways.

### Example 1: Mean of element in a PCollection

We use `Mean.Globally()` to get the average of the elements from the *entire* `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/mean.py" mean_globally >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/mean_test.py" mean_element >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/mean.py" >}}

### Example 2: Mean of elements for each key

We use `Mean.PerKey()` to get the average of the elements for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/mean.py" mean_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/mean_test.py" elements_with_mean_value_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/mean.py" >}}

## Related transforms

* [CombineGlobally](/documentation/transforms/python/aggregation/combineglobally)
* [CombinePerKey](/documentation/transforms/python/aggregation/combineperkey)
* [Max](/documentation/transforms/python/aggregation/max)
* [Min](/documentation/transforms/python/aggregation/min)
* [Sum](/documentation/transforms/python/aggregation/sum)

{{< button-pydoc path="apache_beam.transforms.combiners" class="Mean" >}}
