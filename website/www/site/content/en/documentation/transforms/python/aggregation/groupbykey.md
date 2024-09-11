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

**Example 1**: In the following example, we create a pipeline with a `PCollection` of produce keyed by season.

We use `GroupByKey` to group all the produce for each season.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_GroupByKeySort" show="groupbykey" >}}
{{< /playground >}}

**Example 2**:

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_GroupByKey" show="groupbykey" >}}
{{< /playground >}}

## Related transforms

* [GroupBy](/documentation/transforms/python/aggregation/groupby) for grouping by arbitrary properties of the elements.
* [CombinePerKey](/documentation/transforms/python/aggregation/combineperkey) for combining all values associated with a key to a single result.
* [CoGroupByKey](/documentation/transforms/python/aggregation/cogroupbykey) for multiple input collections.

{{< button-pydoc path="apache_beam.transforms.core" class="GroupByKey" >}}
