---
title: "Flatten"
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

# Flatten
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html?highlight=flatten#apache_beam.transforms.core.Flatten">
      <img src="/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br><br>



Merges multiple `PCollection` objects into a single logical
`PCollection`. A transform for `PCollection` objects
that store the same data type.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#flatten).

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_Flatten" show="flatten" >}}
{{< /playground >}}

## Related transforms
* [FlatMap](/documentation/transforms/python/elementwise/flatmap) applies a simple 1-to-many mapping
  function over each element in the collection. This transform might produce zero
  or more outputs.
