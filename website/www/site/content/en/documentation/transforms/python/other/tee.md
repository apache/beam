---
title: "Tee"
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

# Tee
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html?highwebsite/www/site/content/en/documentation/transforms/python/otherlight=flattenwith#apache_beam.transforms.util.Tee">
      <img src="/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br><br>


The `Tee` transform allows for splitting the pipeline flow into multiple branches,
enabling the application of side transformations while preserving the main pipeline.
This is similar to the Unix `tee` command, which duplicates input and sends it to
multiple outputs without interrupting the main flow.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#tee).

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_Tee" show="tee" >}}
{{< /playground >}}
