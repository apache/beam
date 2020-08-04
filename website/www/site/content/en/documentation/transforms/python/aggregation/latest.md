---
title: "Latest"
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

# Latest

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.combiners" class="Latest" >}}

Gets the element with the latest timestamp.

## Examples

In the following examples, we create a pipeline with a `PCollection` of produce with a timestamp for their harvest date.

We use `Latest` to get the element with the latest timestamp from the `PCollection`.

### Example 1: Latest element globally

We use `Latest.Globally()` to get the element with the latest timestamp in the entire `PCollection`.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/latest.py" latest_globally >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/latest_test.py" latest_element >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/latest.py" >}}

### Example 2: Latest elements for each key

We use `Latest.PerKey()` to get the elements with the latest timestamp for each key in a `PCollection` of key-values.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/latest.py" latest_per_key >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/latest_test.py" latest_elements_per_key >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/latest.py" >}}

## Related transforms

* [Sample](/documentation/transforms/python/aggregation/sample) randomly takes some number of elements in a collection.

{{< button-pydoc path="apache_beam.transforms.combiners" class="Latest" >}}
