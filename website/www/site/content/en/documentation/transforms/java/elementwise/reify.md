---
title: "Reify"
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
# Reify
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Reify.html">
      <img src="https://beam.apache.org/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Transforms for converting between explicit and implicit form of various Beam values.

## Examples
See [BEAM-7702](https://issues.apache.org/jira/browse/BEAM-7702) for updates.

## Related transforms
* [WithTimestamps](/documentation/transforms/java/elementwise/withtimestamps)
  assigns timestamps to all the elements of a collection
* [Window](/documentation/transforms/java/other/window/) divides up or
  groups the elements of a collection into finite windows
