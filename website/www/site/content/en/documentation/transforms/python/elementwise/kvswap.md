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

# Kvswap

{{< localstorage language language-py >}}

{{< button-pydoc path="apache_beam.transforms.util" class="KvSwap" >}}

Takes a collection of key-value pairs and returns a collection of key-value pairs
which has each key and value swapped.

## Examples

In the following example, we create a pipeline with a `PCollection` of key-value pairs.
Then, we apply `KvSwap` to swap the keys and values.

{{< playground height="700px" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_KvSwap" show="kvswap" >}}
{{< /playground >}}

## Related transforms

* [Keys](/documentation/transforms/python/elementwise/keys) for extracting the key of each component.
* [Values](/documentation/transforms/python/elementwise/values) for extracting the value of each element.

{{< button-pydoc path="apache_beam.transforms.util" class="KvSwap" >}}
