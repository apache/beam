---
title: "Create"
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
# Create
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Create.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Creates a collection containing a specified set of elements. This is useful
for testing, as well as creating an initial input to process in parallel.
For example, a single element to execute a one-time `ParDo` or a list of filenames to be read.


## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_Create" show="main_section" >}}
{{< /playground >}}

## Related transforms
N/A
