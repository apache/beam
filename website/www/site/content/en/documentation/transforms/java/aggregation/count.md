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
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Count.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Counts the number of elements within each aggregation. The `Count`
transform has three varieties:

* `Count.globally()` counts the number of elements in the entire
  `PCollection`. The result is a collection with a single element.
* `Count.perKey()` counts how many elements are associated with each
  key. It ignores the values. The resulting collection has one
  output for every key in the input collection.
* `Count.perElement()` counts how many times each element appears
  in the input collection. The output collection is a key-value
  pair, containing each unique element and the number of times it
  appeared in the original collection.

## Examples

**Example 1**: Count.globally

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_Count" show="main_section" >}}
{{< /playground >}}

**Example 2**: Count.perKey

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_CountPerKey" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [ApproximateUnique](/documentation/transforms/java/aggregation/approximateunique)
  estimates the number of distinct elements or distinct values in key-value pairs
* [Sum](/documentation/transforms/java/aggregation/sum) computes
  the sum of elements in a collection
