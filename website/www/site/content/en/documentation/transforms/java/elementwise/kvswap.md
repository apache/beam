---
title: "KvSwap"
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
# KvSwap
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/KvSwap.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Takes a collection of key-value pairs and returns a collection of key-value pairs which has each key and value swapped.

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_KvSwap" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [Keys](/documentation/transforms/java/elementwise/keys) for extracting the key of each component.
* [Values](/documentation/transforms/java/elementwise/values) for extracting the value of each element.
* [WithKeys](/documentation/transforms/java/elementwise/withkeys) for adding a key to each element.
