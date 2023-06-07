---
title: "CoGroupByKey"
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
# CoGroupByKey
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/join/CoGroupByKey.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Aggregates all input elements by their key and allows downstream processing
to consume all values associated with the key. While `GroupByKey` performs
this operation over a single input collection and thus a single type of
input values, `CoGroupByKey` operates over multiple input collections. As
a result, the result for each key is a tuple of the values associated with
that key in each input collection.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#cogroupbykey).

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_GroupByKey" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [GroupByKey](/documentation/transforms/java/aggregation/groupbykey)
  takes one input collection.
