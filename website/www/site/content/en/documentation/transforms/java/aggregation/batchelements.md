---
title: "BatchElements"
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

# BatchElements

BatchElements transform groups individual elements into batches before processing them downstream.
It is designed for operations where each call has a fixed overhead regardless of how many elements are processed and the transform amortizes that cost across multiple elements at once.
The transform takes a `PCollection<T>` as input and produces a `PCollection<List<T>>`, where each output element is a batch containing multiple input elements.
Batch sizes are chosen dynamically between the configured minimum and maximum values by measuring the execution time of downstream operations.

Batching is performed per window. Each emitted batch belongs to the same window as its input elements.

## Examples

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_BatchElements" show="main_section" >}}
{{< /playground >}}
