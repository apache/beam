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
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Mean.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Transforms for computing the arithmetic mean of the elements in a collection,
or the mean of the values associated with each key in a collection of key-value pairs.

* `Mean.globally()` returns a transform that then returns a collection whose contents is the mean of the input collection's elements. If there are no elements in the input collection, it returns 0.
* `Mean.perKey()` returns a transform that returns a collection that contains an output element mapping each distinct key in the input collection to the mean of the values associated with that key in the input collection.

## Examples
**Example 1**: get the mean of a `PCollection` of `Longs`.

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_Mean" show="main_section" >}}
{{< /playground >}}

**Example 2**: calculate the mean of the `Integers` associated with each unique key (which is of type `String`).

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_MeanPerKey" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [Max](/documentation/transforms/java/aggregation/max)
  for computing maximum values in a collection
* [Min](/documentation/transforms/java/aggregation/min)
  for computing maximum values in a collection
* [Combine](/documentation/transforms/java/aggregation/combine)
  for combining all values associated with a key to a single result
