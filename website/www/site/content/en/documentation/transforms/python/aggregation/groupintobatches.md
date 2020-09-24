---
title: "GroupIntoBatches"
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

# GroupIntoBatches

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.util" class="GroupIntoBatches" >}}

Batches the input into desired batch size.

## Examples

In the following example, we create a pipeline with a `PCollection` of produce by season.

We use `GroupIntoBatches` to get fixed-sized batches for every key, which outputs a list of elements for every key.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupintobatches.py" groupintobatches >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupintobatches_test.py" batches_with_keys >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupintobatches.py" >}}

## Related transforms

<!-- TODO(BEAM-10889): Create a page for BatchElements and link to it here. //-->
For unkeyed data and dynamic batch sizes, one may want to use
[BatchElements](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements).

{{< button-pydoc path="apache_beam.transforms.util" class="GroupIntoBatches" >}}
