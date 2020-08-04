---
title: "HllCount"
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
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/extensions/zetasketch/HllCount.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>


Estimates the number of distinct elements in a data stream using the
[HyperLogLog++ algorithm](https://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/40671.pdf).
The respective transforms to create and merge sketches, and to extract from them, are:

* `HllCount.Init` aggregates inputs into HLL++ sketches.
* `HllCount.MergePartial` merges HLL++ sketches into a new sketch.
* `HllCount.Extract` extracts the estimated count of distinct elements from HLL++ sketches.

You can read more about what a sketch is at https://github.com/google/zetasketch.

## Examples
**Example 1**: creates a long-type sketch for a `PCollection<Long>` with a custom precision:
{{< highlight java >}}
 PCollection<Long> input = ...;
 int p = ...;
 PCollection<byte[]> sketch = input.apply(HllCount.Init.forLongs().withPrecision(p).globally());
{{< /highlight >}}

**Example 2**: creates a bytes-type sketch for a `PCollection<KV<String, byte[]>>`:
{{< highlight java >}}
 PCollection<KV<String, byte[]>> input = ...;
 PCollection<KV<String, byte[]>> sketch = input.apply(HllCount.Init.forBytes().perKey());
{{< /highlight >}}

**Example 3**: merges existing sketches in a `PCollection<byte[]>` into a new sketch,
which summarizes the union of the inputs that were aggregated in the merged sketches:
{{< highlight java >}}
 PCollection<byte[]> sketches = ...;
 PCollection<byte[]> mergedSketch = sketches.apply(HllCount.MergePartial.globally());
{{< /highlight >}}

**Example 4**: estimates the count of distinct elements in a `PCollection<String>`:
{{< highlight java >}}
 PCollection<String> input = ...;
 PCollection<Long> countDistinct =
     input.apply(HllCount.Init.forStrings().globally()).apply(HllCount.Extract.globally());
{{< /highlight >}}

**Example 5**: extracts the count distinct estimate from an existing sketch:
{{< highlight java >}}
 PCollection<byte[]> sketch = ...;
 PCollection<Long> countDistinct = sketch.apply(HllCount.Extract.globally());
{{< /highlight >}}

## Related transforms
* [ApproximateUnique](/documentation/transforms/java/aggregation/approximateunique)
  estimates the number of distinct elements or values in key-value pairs (but does not expose sketches; also less accurate than `HllCount`).