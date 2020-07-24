---
title: "GroupByKey"
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

# GroupByKey

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.core" class="GroupByKey" >}}

Takes a keyed collection of elements and produces a collection
where each element consists of a key and all values associated with that key.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#groupbykey).

## Examples

In the following example, we create a pipeline with a `PCollection` of produce keyed by season.

We use `GroupByKey` group all the produce for each season.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupbykey.py" groupbykey >}}
{{< /highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}

{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupbykey_test.py" produce_counts >}}
{{< /highlight >}}

{{< buttons-code-snippet
  py="sdks/python/apache_beam/examples/snippets/transforms/aggregation/groupbykey.py" >}}

## Related transforms

* [CombineGlobally](/documentation/transforms/python/aggregation/combineglobally) for combining all values associated with a key to a single result.
* [CoGroupByKey](/documentation/transforms/python/aggregation/cogroupbykey) for multiple input collections.

{{< button-pydoc path="apache_beam.transforms.core" class="GroupByKey" >}}
