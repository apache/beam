---
title: "GroupByKey"
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
# GroupByKey
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/GroupByKey.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Takes a keyed collection of elements and produces a collection where
each element consists of a key and an `Iterable` of all values
associated with that key.

The results can be combined with windowing to subdivide each key
based on time or triggering to produce partial aggregations. Either
windowing or triggering is necessary when processing unbounded collections.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#groupbykey).

## Examples
**Example 1**: (a, 1), (b, 2), (a, 3) will result into (a, [1, 3]), (b, [2]).

**Example 2**: Given a collection of customer orders keyed by postal code,
you could use `GroupByKey` to get the collection of all orders in each postal code.

## Related transforms
* [CoGroupByKey](/documentation/transforms/java/aggregation/cogroupbykey)
  for multiple input collections
* [Combine](/documentation/transforms/java/aggregation/combine)
  for combining all values associated with a key to a single result
