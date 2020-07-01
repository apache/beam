---
title: "Combine"
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
# Combine
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Combine.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

A user-defined `CombineFn` may be applied to combine all elements in a
`PCollection` (global combine) or to combine all elements associated
with each key. 

While the result is similar to applying a `GroupByKey` followed by
aggregating values in each `Iterable`, there is an impact
on the code you must write as well as the performance of the pipeline.
Writing a `ParDo` that counts the number of elements in each value
would be very straightforward. However, as described in the execution
model, it would also require all values associated with each key to be
processed by a single worker. This introduces a lot of communication overhead.
Using a `CombineFn` requires the code be structured as an associative and
commumative operation. But, it allows the use of partial sums to be precomputed.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#combine).

## Examples
**Example 1**: Global combine
Use the global combine to combine all of the elements in a given `PCollection`
into a single value, represented in your pipeline as a new `PCollection` containing
one element. The following example code shows how to apply the Beam-provided
sum combine function to produce a single sum value for a `PCollection` of integers.

{{< highlight java >}}
// Sum.SumIntegerFn() combines the elements in the input PCollection. The resulting PCollection, called sum,
// contains one value: the sum of all the elements in the input PCollection.
PCollection<Integer> pc = ...;
PCollection<Integer> sum = pc.apply(
   Combine.globally(new Sum.SumIntegerFn()));
{{< /highlight >}}

**Example 2**: Keyed combine
Use a keyed combine to combine all of the values associated with each key
into a single output value for each key. As with the global combine, the
function passed to a keyed combine must be associative and commutative.

{{< highlight java >}}
// PCollection is grouped by key and the Double values associated with each key are combined into a Double.
PCollection<KV<String, Double>> salesRecords = ...;
PCollection<KV<String, Double>> totalSalesPerPerson =
  salesRecords.apply(Combine.<String, Double, Double>perKey(
    new Sum.SumDoubleFn()));
// The combined value is of a different type than the original collection of values per key. PCollection has
// keys of type String and values of type Integer, and the combined value is a Double.
PCollection<KV<String, Integer>> playerAccuracy = ...;
PCollection<KV<String, Double>> avgAccuracyPerPlayer =
  playerAccuracy.apply(Combine.<String, Integer, Double>perKey(
    new MeanInts())));
{{< /highlight >}}

## Related transforms 
* [CombineWithContext](/documentation/transforms/java/aggregation/combinewithcontext)
* [GroupByKey](/documentation/transforms/java/aggregation/groupbykey) 
