---
title: "ApproximateUnique"
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
# ApproximateUnique
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/ApproximateUnique.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Transforms for estimating the number of distinct elements in a collection
or the number of distinct values associated with each key in a collection
of key-value pairs.

## Examples
See [BEAM-7703](https://issues.apache.org/jira/browse/BEAM-7703) for updates.

## Related transforms 
* [HllCount](/documentation/transforms/java/aggregation/hllcount)
  estimates the number of distinct elements and creates re-aggregatable sketches using the HyperLogLog++ algorithm.
* [Count](/documentation/transforms/java/aggregation/count)
  counts the number of elements within each aggregation.
* [Distinct](/documentation/transforms/java/aggregation/distinct)