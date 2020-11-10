---
title: "GroupIntoBatches"
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
# GroupIntoBatches
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/GroupIntoBatches.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Batches inputs to a desired batch size.

Batches contain only elements of a single key. Elements are buffered until
`batchSize` number of elements buffered. Then, these elements are output
to the output collection.

Batches contain elements from the same window, so windows are preserved. Batches might contain elements from more than one bundle.

## Examples
See [BEAM-7703](https://issues.apache.org/jira/browse/BEAM-7703) for updates.

## Related transforms
* [GroupByKey](/documentation/transforms/java/aggregation/groupbykey) takes one input collection.
