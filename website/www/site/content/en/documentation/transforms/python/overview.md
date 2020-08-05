---
title: "Python transform catalog overview"
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

# Python transform catalog overview

## Element-wise

<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/filter">Filter</a></td><td>Given a predicate, filter out all elements that don't satisfy the predicate.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/flatmap">FlatMap</a></td><td>Applies a function that returns a collection to every element in the input and
  outputs all resulting elements.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/keys">Keys</a></td><td>Extracts the key from each element in a collection of key-value pairs.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/kvswap">KvSwap</a></td><td>Swaps the key and value of each element in a collection of key-value pairs.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/map">Map</a></td><td>Applies a function to every element in the input and outputs the result.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/pardo">ParDo</a></td><td>The most-general mechanism for applying a user-defined <code>DoFn</code> to every element
  in the input collection.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/partition">Partition</a></td><td>Routes each input element to a specific output collection based on some partition
  function.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/regex">Regex</a></td><td>Filters input string elements based on a regex. May also transform them based on the matching groups.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/reify">Reify</a></td><td>Transforms for converting between explicit and implicit form of various Beam values.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/tostring">ToString</a></td><td>Transforms every element in an input collection a string.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/withtimestamps">WithTimestamps</a></td><td>Applies a function to determine a timestamp to each element in the output collection,
  and updates the implicit timestamp associated with each input. Note that it is only
  safe to adjust timestamps forwards.</td></tr>
  <tr><td><a href="/documentation/transforms/python/elementwise/values">Values</a></td><td>Extracts the value from each element in a collection of key-value pairs.</td></tr>
</table>

## Aggregation

<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>
  <tr><td>ApproximateQuantiles</td><td>Not available. See <a href="https://issues.apache.org/jira/browse/BEAM-6694">BEAM-6694</a> for updates.</td></tr>
  <tr><td>ApproximateUnique</td><td>Not available. See <a href="https://issues.apache.org/jira/browse/BEAM-6693">BEAM-6693</a> for updates.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/cogroupbykey">CoGroupByKey</a></td><td>Takes several keyed collections of elements and produces a collection where each element consists of a key and all values associated with that key.</td></tr>  
  <tr><td><a href="/documentation/transforms/python/aggregation/combineglobally">CombineGlobally</a></td><td>Transforms to combine elements.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/combineperkey">CombinePerKey</a></td><td>Transforms to combine elements for each key.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/combinevalues">CombineValues</a></td><td>Transforms to combine keyed iterables.</td></tr>
  <tr><td>CombineWithContext</td><td>Not available.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/count">Count</a></td><td>Counts the number of elements within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/distinct">Distinct</a></td><td>Produces a collection containing distinct elements from the input collection.</td></tr>  
  <tr><td><a href="/documentation/transforms/python/aggregation/groupbykey">GroupByKey</a></td><td>Takes a keyed collection of elements and produces a collection where each element consists of a key and all values associated with that key.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/groupintobatches">GroupIntoBatches</a></td><td>Batches the input into desired batch size.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/latest">Latest</a></td><td>Gets the element with the latest timestamp.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/max">Max</a></td><td>Gets the element with the maximum value within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/mean">Mean</a></td><td>Computes the average within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/min">Min</a></td><td>Gets the element with the minimum value within each aggregation.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/sample">Sample</a></td><td>Randomly select some number of elements from each aggregation.</td></tr>
  <tr><td>Sum</td><td>Not available.</td></tr>
  <tr><td><a href="/documentation/transforms/python/aggregation/top">Top</a></td><td>Compute the largest element(s) in each aggregation.</td></tr>
</table>

## Other

<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>
  <tr><td><a href="/documentation/transforms/python/other/create">Create</a></td><td>Creates a collection from an in-memory list.</td></tr>
  <tr><td><a href="/documentation/transforms/python/other/flatten">Flatten</a></td><td>Given multiple input collections, produces a single output collection containing
  all elements from all of the input collections.
</td></tr>
  <tr><td>PAssert</td><td>Not available.</td></tr>
  <tr><td><a href="/documentation/transforms/python/other/reshuffle">Reshuffle</a></td><td>Given an input collection, redistributes the elements between workers. This is
  most useful for adjusting parallelism or preventing coupled failures.</td></tr>
  <tr><td>View</td><td>Not available.</td></tr>
  <tr><td><a href="/documentation/transforms/python/other/windowinto">WindowInto</a></td><td>Logically divides up or groups the elements of a collection into finite
  windows according to a function.</td></tr>
</table>
