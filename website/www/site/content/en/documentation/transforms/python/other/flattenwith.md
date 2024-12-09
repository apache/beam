---
title: "FlattenWith"
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

# FlattenWith
<table align="left">
    <a target="_blank" class="button"
        href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html?highwebsite/www/site/content/en/documentation/transforms/python/otherlight=flattenwith#apache_beam.transforms.core.FlattenWith">
      <img src="/images/logos/sdks/python.png" width="20px" height="20px"
           alt="Pydoc" />
     Pydoc
    </a>
</table>
<br><br>


Merges multiple `PCollection` objects into a single logical
`PCollection`. It allows for the combination of both root
`PCollection`-producing transforms (like `Create` and `Read`) and existing
PCollections.

See more information in the [Beam Programming Guide](/documentation/programming-guide/#flattenwith).

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_FlattenWith" show="flattenwith" >}}
{{< /playground >}}

## Related transforms
* [Flatten](/documentation/transforms/python/other/flatten) merges multiple
`PCollection` objects into a single logical `PCollection`. This is useful when
dealing with multiple collections of the same data type.
* [FlatMap](/documentation/transforms/python/elementwise/flatmap) applies a
simple 1-to-many mapping function over each element in the collection. This
transform might produce zero or more outputs.
