---
title: "Top"
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

# Top

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.combiners" class="Top" >}}

Transforms for finding the largest (or smallest) set of elements in
a collection, or the largest (or smallest) set of values associated
with each key in a collection of key-value pairs.

## Examples

In the following example, we create a pipeline with a `PCollection`.
Then, we get the largest or smallest elements in different ways.

### Example 1: Largest elements from a PCollection

We use `Top.Largest()` to get the largest elements from the *entire* `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" top_largest >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top_test.py" largest_elements >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" >}}

### Example 2: Largest elements for each key

We use `Top.LargestPerKey()` to get the largest elements for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" top_largest_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top_test.py" largest_elements_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" >}}

### Example 3: Smallest elements from a PCollection

We use `Top.Smallest()` to get the smallest elements from the *entire* `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" top_smallest >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top_test.py" smallest_elements >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" >}}

### Example 4: Smallest elements for each key

We use `Top.SmallestPerKey()` to get the smallest elements for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" top_smallest_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top_test.py" smallest_elements_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" >}}

### Example 5: Custom elements from a PCollection

We use `Top.Of()` to get elements with customized rules from the *entire* `PCollection`.

You can change how the elements are compared with `key`.
By default you get the largest elements, but you can get the smallest by setting `reverse=True`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" top_of >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top_test.py" shortest_elements >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" >}}

### Example 6: Custom elements for each key

We use `Top.PerKey()` to get elements with customized rules for each unique key in a `PCollection` of key-values.

You can change how the elements are compared with `key`.
By default you get the largest elements, but you can get the smallest by setting `reverse=True`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" top_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/top_test.py" shortest_elements_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/top.py" >}}

## Related transforms

* [Sample](/documentation/transforms/python/aggregation/sample) to combine elements. takes samples of the elements in a collection.

{{< button-pydoc path="apache_beam.transforms.combiners" class="Top" >}}
