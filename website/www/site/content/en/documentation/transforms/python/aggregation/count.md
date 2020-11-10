---
title: "Count"
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

# Count

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.combiners" class="Count" >}}

Counts the number of elements within each aggregation.

## Examples

In the following example, we create a pipeline with two `PCollection`s of produce.
Then, we apply `Count` to get the total number of elements in different ways.

### Example 1: Counting all elements in a PCollection

We use `Count.Globally()` to count *all* elements in a `PCollection`, even if there are duplicate elements.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/count.py" count_globally >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/count_test.py" total_elements >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/count.py" >}}

### Example 2: Counting elements for each key

We use `Count.PerKey()` to count the elements for each unique key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/count.py" count_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/count_test.py" total_elements_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/count.py" >}}

### Example 3: Counting all unique elements

We use `Count.PerElement()` to count the only the unique elements in a `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/count.py" count_per_element >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/count_test.py" total_unique_elements >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/count.py" >}}

## Related transforms

N/A

{{< button-pydoc path="apache_beam.transforms.combiners" class="Count" >}}
