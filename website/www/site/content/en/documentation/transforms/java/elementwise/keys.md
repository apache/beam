---
title: "Keys"
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
# Keys
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/transforms/Keys.html">
      <img src="/images/logos/sdks/java.png" width="20px" height="20px"
           alt="Javadoc" />
     Javadoc
    </a>
</table>
<br><br>

Takes a collection of key-value pairs, and returns the key of each element.

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_Keys" show="main_section" >}}
{{< /playground >}}

## Related transforms
* [KvSwap](/documentation/transforms/java/elementwise/kvswap) swaps key-value pair values.
* [Values](/documentation/transforms/java/elementwise/values) for extracting the value of each element.
* [WithKeys](/documentation/transforms/java/elementwise/withkeys) for adding a key to each element.
