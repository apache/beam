---
title: "Max"
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
# Max
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Max.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Provides a variety of different transforms for computing the maximum
values in a collection, either globally or for each key.

## Examples
**Example 1**: get the maximum of a `PCollection` of `Doubles`.

{{< highlight java >}}
PCollection<Double> input = ...;
PCollection<Double> max = input.apply(Max.doublesGlobally());
{{< /highlight >}}

**Example 2**: calculate the maximum of the `Integers` associated
with each unique key (which is of type `String`).

{{< highlight java >}}
PCollection<KV<String, Integer>> input = ...;
PCollection<KV<String, Integer>> maxPerKey = input
     .apply(Max.integersPerKey());
{{< /highlight >}}

## Related transforms 
* [Min](/documentation/transforms/java/aggregation/min)
  for computing minimum values in a collection
* [Mean](/documentation/transforms/java/aggregation/mean)
  for computing the arithmetic mean of the elements in a collection
* [Combine](/documentation/transforms/java/aggregation/combine)
  for combining all values associated with a key to a single result