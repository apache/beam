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

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.util" class="Distinct" >}}

Produces a collection containing distinct elements of the input collection.

## Examples

In the following example, we create a pipeline with two `PCollection`s of produce.

We use `Distinct` to get rid of duplicate elements, which outputs a `PCollection` of all the unique elements.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_Distinct" show="distinct" >}}
{{< /playground >}}

## Related transforms

* [Count](/documentation/transforms/python/aggregation/count) counts the number of elements within each aggregation.

{{< button-pydoc path="apache_beam.transforms.util" class="Distinct" >}}
