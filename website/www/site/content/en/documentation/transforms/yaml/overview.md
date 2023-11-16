---
title: "YAML transform catalog overview"
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

# YAML transform catalog overview

## Element-wise

<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/elementwise/maptofields">MapToFields</a></td><td>Given a set of fields and corresponding mapping functions, applies the given functions per field on every element and outputs the resulting elements as a Beam Row with the given fields.</td></tr>)
  <tr><td><a href="/documentation/transforms/yaml/elementwise/filter">Filter</a></td><td>Given a predicate, filter out all elements that don't satisfy the predicate.</td></tr>

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/elementwise/explode">Explode</a></td><td>Transforms elements with iterable values into corresponding sets of elements for each value in the value iterable.</td></tr>)
</table>

## Aggregation (Experimental)

<table class="table-bordered table-striped">
  <tr><th>Transform</th><th>Description</th></tr>

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/other/sql">Sql</a></td><td>Invokes the <a href="/documentation/dsls/sql/overview">SqlTransform</a> using the given query on the input collection.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/aggregation/combine">Combine</a></td><td>Transform to group and combine values across records.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/sum">Sum</a></td><td>Sums all the elements within each aggregation.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/max">Max</a></td><td>Gets the element with the maximum value within each aggregation.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/min">Min</a></td><td>Gets the element with the minimum value within each aggregation.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/all">All</a></td><td>Returns true if all the elements are true within each aggregation.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/any">Any</a></td><td>Returns true if any of the elements are true within each aggregation.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/mean">Mean</a></td><td>Computes the average within each aggregation.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/python/aggregation/count">Count</a></td><td>Counts the number of elements within each aggregation.</td></tr>)
</table>

## Other

<table class="table-bordered table-striped">

[//]: # (  <tr><th>Transform</th><th>Description</th></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/other/create">Create</a></td><td>Creates a collection from an in-memory list.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/other/flatten">Flatten</a></td><td>Given multiple input collections, produces a single output collection containing)

[//]: # (  all elements from all of the input collections.)

[//]: # (</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/other/windowinto">WindowInto</a></td><td>Logically divides up or groups the elements of a collection into finite)

[//]: # (  windows according to a function.</td></tr>)

[//]: # (  <tr><td><a href="/documentation/transforms/yaml/other/pytransform">PyTransform</a></td><td>Takes the fully qualified name of a Beam Python transform and performs the transform using the parameters provided in the config.</td></tr>)
</table>
