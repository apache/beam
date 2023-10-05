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

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_TopLargest" show="top_largest" >}}
{{< /playground >}}

### Example 2: Largest elements for each key

We use `Top.LargestPerKey()` to get the largest elements for each unique key in a `PCollection` of key-values.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_TopLargestPerKey" show="top_largest_per_key" >}}
{{< /playground >}}

### Example 3: Smallest elements from a PCollection

We use `Top.Smallest()` to get the smallest elements from the *entire* `PCollection`.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_TopSmallest" show="top_smallest" >}}
{{< /playground >}}

### Example 4: Smallest elements for each key

We use `Top.SmallestPerKey()` to get the smallest elements for each unique key in a `PCollection` of key-values.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_TopSmallestPerKey" show="top_smallest_per_key" >}}
{{< /playground >}}

### Example 5: Custom elements from a PCollection

We use `Top.Of()` to get elements with customized rules from the *entire* `PCollection`.

You can change how the elements are compared with `key`.
By default you get the largest elements, but you can get the smallest by setting `reverse=True`.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_TopOf" show="top_of" >}}
{{< /playground >}}

### Example 6: Custom elements for each key

We use `Top.PerKey()` to get elements with customized rules for each unique key in a `PCollection` of key-values.

You can change how the elements are compared with `key`.
By default you get the largest elements, but you can get the smallest by setting `reverse=True`.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_TopPerKey" show="top_per_key" >}}
{{< /playground >}}

## Related transforms

* [Sample](/documentation/transforms/python/aggregation/sample) to combine elements. Takes samples of the elements in a collection.

{{< button-pydoc path="apache_beam.transforms.combiners" class="Top" >}}
