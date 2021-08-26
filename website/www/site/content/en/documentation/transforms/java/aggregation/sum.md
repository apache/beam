---
title: "Sum"
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
# Sum
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Sum.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Transforms for computing the sum of the elements in a collection, or the sum of the
values associated with each key in a collection of key-value pairs.

## Examples
**Example 1**: get the sum of a `PCollection` of `Doubles`.

{{< highlight java >}}
PCollection<Double> input = ...;
PCollection<Double> sum = input.apply(Sum.doublesGlobally());
{{< /highlight >}}

Example 2: calculate the sum of the `Integers` associated with each unique key (which is of type `String`).

{{< highlight java >}}
PCollection<KV<String, Integer>> input = ...;
PCollection<KV<String, Integer>> sumPerKey = input
     .apply(Sum.integersPerKey());
{{< /highlight >}}

## Related transforms
* [Count](/documentation/transforms/java/aggregation/count)
  counts the number of elements within each aggregation
