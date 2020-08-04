---
title: "Distinct"
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
# Distinct
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Distinct.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Produces a collection containing distinct elements of the input collection.

On some data sets, it might be more efficient to compute an approximate
answer using `ApproximateUnique`, which also allows for determining distinct
values for each key.

## Examples
See [BEAM-7703](https://issues.apache.org/jira/browse/BEAM-7703) for updates.

## Related transforms 
* [Count](/documentation/transforms/java/aggregation/count)
  counts the number of elements within each aggregation.
* [ApproximateUnique](/documentation/transforms/java/aggregation/approximateunique)
  estimates the number of distinct elements in a collection.