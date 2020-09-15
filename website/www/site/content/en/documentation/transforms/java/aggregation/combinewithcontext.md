---
title: "CombineWithContext"
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
# CombineWithContext
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/CombineWithContext.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

A class of transforms that contains combine functions that have access to `PipelineOptions` and side inputs through `CombineWithContext.Context`.

## Examples
See [BEAM-7703](https://issues.apache.org/jira/browse/BEAM-7703) for updates.

## Related transforms
* [Combine](/documentation/transforms/java/aggregation/combine)
  for combining all values associated with a key to a single result
