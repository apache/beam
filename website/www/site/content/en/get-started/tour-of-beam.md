---
title: "Tour of Beam"
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

# Tour of Beam

Here you can find a collection of the interactive notebooks available for Apache Beam, which are hosted in
[Colab](https://colab.research.google.com).
The notebooks allow you to interactively play with the code and see how your changes affect the pipeline.
You don't need to install anything or modify your computer in any way to use these notebooks.

You can also [try an Apache Beam pipeline](/get-started/try-apache-beam) using the Java, Python, and Go SDKs.

## Get started

{{< table >}}
<table>
  <col style="width: auto;">
  <col style="width: 200px;">
  {{< row-colab
    title="Learn the basics"
    body="In this notebook we go through the basics of what is Apache Beam and how to get started."
    button_url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/getting-started.ipynb" >}}
</table>
{{< /table >}}

## Transforms

Check the [Python transform catalog](/documentation/transforms/python/overview/)
for a complete list of the available transforms.

### Element-wise transforms

{{< table >}}
<table>
  <col style="width: auto;">
  <col style="width: 200px;">
  {{< row-colab
    title="Map"
    body="Applies a simple one-to-one mapping function over each element in the collection."
    button_url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/map-py.ipynb" >}}
  {{< row-colab
    title="FlatMap"
    body="Applies a simple one-to-many mapping function over each element in the collection. The many elements are flattened into the resulting collection."
    button_url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/flatmap-py.ipynb" >}}
  {{< row-colab
    title="Filter"
    body="Given a predicate, filter out all elements that don’t satisfy that predicate."
    button_url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/filter-py.ipynb" >}}
  {{< row-colab
    title="Partition"
    body="Separates elements in a collection into multiple output collections."
    button_url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/partition-py.ipynb" >}}
  {{< row-colab
    title="ParDo"
    body="A transform for generic parallel processing. It's recommended to use Map, FlatMap, Filter or other more specific transforms when possible."
    button_url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/pardo-py.ipynb" >}}
</table>
{{< /table >}}
