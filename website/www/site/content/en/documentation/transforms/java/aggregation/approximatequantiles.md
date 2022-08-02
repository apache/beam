---
title: "ApproximateQuantiles"
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
# ApproximateQuantiles
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/ApproximateQuantiles.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Takes a comparison function and the desired number of quantiles *n*, either
globally or per-key. Using an approximation algorithm, it returns the
minimum value, *n-2* intermediate values, and the maximum value.

## Examples
**Example**: to compute the quartiles of a `PCollection` of integers, we
would use `ApproximateQuantiles.globally(5)`. This will produce a list
containing 5 values: the minimum value, Quartile 1 value, Quartile 2
value, Quartile 3 value, and the maximum value.

## Related transforms
* [ApproximateUnique](/documentation/transforms/java/aggregation/approximateunique)
  estimates the number of distinct elements or distinct values in key-value pairs
* [Combine](/documentation/transforms/java/aggregation/combine)
