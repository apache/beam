---
title: "Java transform catalog overview"
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

# Java transform catalog overview

## Element-wise

<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/filter">Filter</a></td><td>Given a predicate, filter out all elements that don't satisfy the predicate.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/flatmapelements">FlatMapElements</a></td><td>Applies a function that returns a collection to every element in the input and
  outputs all resulting elements.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/keys">Keys</a></td><td>Extracts the key from each element in a collection of key-value pairs.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/kvswap">KvSwap</a></td><td>Swaps the key and value of each element in a collection of key-value pairs.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/mapelements">MapElements</a></td><td>Applies a function to every element in the input and outputs the result.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/pardo">ParDo</a></td><td>The most-general mechanism for applying a user-defined <code>DoFn</code> to every element
  in the input collection.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/partition">Partition</a></td><td>Routes each input element to a specific output collection based on some partition
  function.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/regex">Regex</a></td><td>Filters input string elements based on a regex. May also transform them based on the matching groups.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/reify">Reify</a></td><td>Transforms for converting between explicit and implicit form of various Beam values.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/tostring">ToString</a></td><td>Transforms every element in an input collection to a string.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/withkeys">WithKeys</a></td><td>Produces a collection containing each element from the input collection converted to a key-value pair, with a key selected by applying a function to the input element.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/withtimestamps">WithTimestamps</a></td><td>Applies a function to determine a timestamp to each element in the output collection,
  and updates the implicit timestamp associated with each input. Note that it is only safe to adjust timestamps forwards.</td></tr>
  <tr><td><a href="/documentation/transforms/java/elementwise/values">Values</a></td><td>Extracts the value from each element in a collection of key-value pairs.</td></tr>
</table>



## Aggregation
<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/approximatequantiles">ApproximateQuantiles</a></td><td>Uses an approximation algorithm to estimate the data distribution within each aggregation using a specified number of quantiles.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/approximateunique">ApproximateUnique</a></td><td>Uses an approximation algorithm to estimate the number of unique elements within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/cogroupbykey/">CoGroupByKey</a></td><td>Similar to <code>GroupByKey</code>, but groups values associated with each key into a batch of a given size</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/combine">Combine</a></td><td>Transforms to combine elements according to a provided <code>CombineFn</code>.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/combinewithcontext">CombineWithContext</a></td><td>An extended version of Combine which allows accessing side-inputs and other context.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/count">Count</a></td><td>Counts the number of elements within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/distinct">Distinct</a></td><td>Produces a collection containing distinct elements from the input collection.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/groupbykey">GroupByKey</a></td><td>Takes a keyed collection of elements and produces a collection where each element
  consists of a key and all values associated with that key.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/groupintobatches">GroupIntoBatches</a></td><td>Batches values associated with keys into <code>Iterable</code> batches of some size. Each batch contains elements associated with a specific key.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/hllcount">HllCount</a></td><td>Estimates the number of distinct elements and creates re-aggregatable sketches using the HyperLogLog++ algorithm.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/latest">Latest</a></td><td>Selects the latest element within each aggregation according to the implicit timestamp.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/max">Max</a></td><td>Outputs the maximum element within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/mean">Mean</a></td><td>Computes the average within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/min">Min</a></td><td>Outputs the minimum element within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/sample">Sample</a></td><td>Randomly select some number of elements from each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/sum">Sum</a></td><td>Compute the sum of elements in each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/java/aggregation/top">Top</a></td><td>Compute the largest element(s) in each aggregation.</td></tr>
</table>


## Other
<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>
  <tr><td><a href="/documentation/transforms/java/other/create">Create</a></td><td>Creates a collection from an in-memory list.</td></tr>
  <tr><td><a href="/documentation/transforms/java/other/flatten">Flatten</a></td><td>Given multiple input collections, produces a single output collection containing
  all elements from all of the input collections.</td></tr>
  <tr><td><a href="/documentation/transforms/java/other/passert">PAssert</a></td><td>A transform to assert the contents of a <code>PCollection</code> used as part of testing a pipeline either locally or with a runner.</td></tr>
  <tr><td><a href="/documentation/transforms/java/other/view">View</a></td><td>Operations for turning a collection into view that may be used as a side-input to a <code>ParDo</code>.</td></tr>
  <tr><td><a href="/documentation/transforms/java/other/window">Window</a></td><td>Logically divides up or groups the elements of a collection into finite
  windows according to a provided <code>WindowFn</code>.</td></tr>
</table>
