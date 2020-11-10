---
title: "Sample"
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

# Sample

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.combiners" class="Sample" >}}

Transforms for taking samples of the elements in a collection, or
samples of the values associated with each key in a collection of
key-value pairs.

## Examples

In the following example, we create a pipeline with a `PCollection`.
Then, we get a random sample of elements in different ways.

### Example 1: Sample elements from a PCollection

We use `Sample.FixedSizeGlobally()` to get a fixed-size random sample of elements from the *entire* `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sample.py" sample_fixed_size_globally >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sample_test.py" sample >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/sample.py" >}}

### Example 2: Sample elements for each key

We use `Sample.FixedSizePerKey()` to get fixed-size random samples for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sample.py" sample_fixed_size_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/sample_test.py" samples_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/sample.py" >}}

## Related transforms

* [Top](/documentation/transforms/python/aggregation/top) finds the largest or smallest element.

{{< button-pydoc path="apache_beam.transforms.combiners" class="Sample" >}}
