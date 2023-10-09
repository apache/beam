---
title: "Partition"
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
# Partition
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Partition.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Separates elements in a collection into multiple output collections. The partitioning function contains the logic that determines how to separate the elements of the input collection into each resulting partition output collection.

The number of partitions must be determined at graph construction time. You cannot determine the number of partitions in mid-pipeline.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#partition).

## Examples

**Example**: dividing a `PCollection` into percentile groups

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_PartitionPercentile" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [Filter](/documentation/transforms/java/elementwise/filter) is useful if the function is just
  deciding whether to output an element or not.
* [ParDo](/documentation/transforms/java/elementwise/pardo) is the most general element-wise mapping
  operation, and includes other abilities such as multiple output collections and side-inputs.
* [CoGroupByKey](/documentation/transforms/java/aggregation/cogroupbykey)
  performs a per-key equijoin.
