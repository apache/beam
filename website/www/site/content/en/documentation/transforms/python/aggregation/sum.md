---
title: "Sum"
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

# Sum

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="CombineGlobally" >}}

Sums all the elements within each aggregation.

## Examples

In the following example, we create a pipeline with a `PCollection`.
Then, we get the sum of all the element values in different ways.

### Example 1: Sum of the elements in a PCollection

We use `Combine.Globally()` to get sum of all the element values from the *entire* `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sum.py" sum_globally >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sum_test.py" total >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/sum.py" >}}

### Example 2: Sum of the elements for each key

We use `Combine.PerKey()` to get the sum of all the element values for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sum.py" sum_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sum_test.py" totals_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/sum.py" >}}

## Related transforms

* [CombineGlobally](/documentation/transforms/python/aggregation/combineglobally)
* [CombinePerKey](/documentation/transforms/python/aggregation/combineperkey)
* [Max](/documentation/transforms/python/aggregation/max)
* [Mean](/documentation/transforms/python/aggregation/mean)
* [Min](/documentation/transforms/python/aggregation/min)

{{< button-pydoc path="apache_beam.transforms.core" class="CombineGlobally" >}}
