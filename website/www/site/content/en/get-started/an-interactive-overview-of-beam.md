---
title: "An Interactive Overview of Beam"
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

# An Interactive Overview of Beam

Here you can find a collection of the interactive notebooks available for Apache Beam, which are hosted in
[Colab](https://colab.research.google.com).
The notebooks allow you to interactively play with the code and see how your changes affect the pipeline.
You don't need to install anything or modify your computer in any way to use these notebooks.

You can also [try an Apache Beam pipeline](/get-started/try-apache-beam) using the Java, Python, and Go SDKs.

## Get started

### Learn the basics

In this notebook we go through the basics of what is Apache Beam and how to get started.
We learn what is a data pipeline, a PCollection, a PTransform, as well as some basic transforms like `Map`, `FlatMap`, `Filter`, `Combine`, and `GroupByKey`.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/getting-started.ipynb" >}}

### Reading and writing data

In this notebook we go through some examples on how to read and write data to and from different data formats.
We introduce the built-in `ReadFromText` and `WriteToText` transforms.
We also see how we can read from CSV files, read from a SQLite database, write fixed-sized batches of elements, and write windows of elements.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/reading-and-writing-data.ipynb" >}}

### Windowing

In this notebook we go through how to aggregate data based on time intervals, or in streaming pipelines.
We introduce the `GlobalWindow`, `FixedWindows`, `SlidingWindows`, and `Sessions`.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/windowing.ipynb" >}}

### DataFrames

Beam DataFrames provide a pandas-like [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html)
API to declare Beam pipelines.
To learn more about Beam DataFrames, take a look at the
[Beam DataFrames overview](/documentation/dsls/dataframes/overview) page.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/interactive-overview/dataframes.ipynb" >}}

## Transforms

Check the [Python transform catalog](/documentation/transforms/python/overview/)
for a complete list of the available transforms.

### Element-wise transforms

#### Map

Applies a simple one-to-one mapping function over each element in the collection.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/map-py.ipynb" >}}

#### FlatMap

Applies a simple one-to-many mapping function over each element in the collection. The many elements are flattened into the resulting collection.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/flatmap-py.ipynb" >}}

#### Filter

Given a predicate, filter out all elements that don’t satisfy that predicate.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/filter-py.ipynb" >}}

#### Partition

Separates elements in a collection into multiple output collections.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/partition-py.ipynb" >}}

#### ParDo

A transform for generic parallel processing. It's recommended to use `Map`, `FlatMap`, `Filter` or other more specific transforms when possible.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/documentation/transforms/python/elementwise/pardo-py.ipynb" >}}
