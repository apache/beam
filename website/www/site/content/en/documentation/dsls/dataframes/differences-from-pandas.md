---
type: languages
title: "Differences from pandas"
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

# Differences from pandas

The Apache Beam DataFrame API aims to be a drop-in replacement for pandas, but there are a few differences to be aware of. This page describes divergences between the Beam and pandas APIs and provides tips for working with the Beam DataFrame API. See the [`apache_beam.dataframe.frames` API reference](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html) for a full reference for which operations and arguments are supported in the Beam DataFrame API.

## Working with pandas sources

Beam operations are always associated with a pipeline. To read source data into a Beam DataFrame, you have to apply the source to a pipeline object. For example, to read input from a CSV file, you could use [read_csv](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.dataframe.io.html#apache_beam.dataframe.io.read_csv) as follows:

    df = p | beam.dataframe.io.read_csv(...)

This is similar to pandas [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html), but `df` is a deferred Beam DataFrame representing the contents of the file. The input filename can be any file pattern understood by [fileio.MatchFiles](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.fileio.html#apache_beam.io.fileio.MatchFiles).

For an example of using sources and sinks with the DataFrame API, see [taxiride.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/dataframe/taxiride.py).

## Classes of unsupported operations

The sections below describe classes of operations that are not yet supported, or supported with caveats, by the Beam DataFrame API. Workarounds are suggested where applicable.

### Non-parallelizable operations

Examples:
[`DeferredDataFrame.quantile`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.quantile),
[`DeferredDataFrame.mode`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.mode)

To support distributed processing, Beam invokes DataFrame operations on subsets of data in parallel. Some DataFrame operations can’t be parallelized, and these operations raise a [NonParallelOperation](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.dataframe.expressions.html#apache_beam.dataframe.expressions.NonParallelOperation) error by default.

**Workaround**

If you want to use a non-parallelizable operation, you can guard it with a `beam.dataframe.allow_non_parallel_operations` block. For example:

    from apache_beam import dataframe

    with dataframe.allow_non_parallel_operations():
      quantiles = df.quantile()

Note that this collects the entire input dataset on a single node, so there’s a risk of running out of memory. You should only use this workaround if you’re sure that the input is small enough to process on a single worker.

### Operations that produce non-deferred columns

Examples:
[`DeferredDataFrame.pivot`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.pivot),
[`DeferredDataFrame.transpose`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.transpose),
[`DeferredSeries.factorize`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredSeries.factorize)

Beam DataFrame operations are deferred, but the schemas of the resulting DataFrames are not, meaning that result columns must be computable without access to the data. Some DataFrame operations can’t support this usage, so they can’t be implemented. These operations raise a [WontImplementError](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.dataframe.frame_base.html#apache_beam.dataframe.frame_base.WontImplementError).

<!-- TODO(https://github.com/apache/beam/issues/20958): Document the use of categorical columns as a workaround -->
Currently there’s no workaround for this issue. But in the future, Beam Dataframe may support non-deferred column operations on categorical columns. This work is being tracked in [Issue 20958](https://github.com/apache/beam/issues/20958).

### Operations that produce non-deferred values or plots

Examples:
[`DeferredSeries.to_list`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredSeries.to_list),
[`DeferredSeries.array`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredSeries.array),
[`DeferredDataFrame.plot`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.plot)

It’s infeasible to implement DataFrame operations that produce non-deferred values or plots because Beam is a deferred API. If these operations are invoked, they will raise a [WontImplementError](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.dataframe.frame_base.html#apache_beam.dataframe.frame_base.WontImplementError).

These operations may be supported in the future through a tighter integration
with Interactive Beam. To track progress on this issue, follow
[Issue 21638](https://github.com/apache/beam/issues/21638). If you think we
should prioritize this work you can also [contact
us](/community/contact-us/) to let us know.

**Workaround**

If you’re using [Interactive Beam](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.runners.interactive.interactive_beam.html), you can use `collect` to bring a dataset into local memory and then perform these operations.

### Order-sensitive operations

Examples:
[`DeferredDataFrame.head`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.head),
[`DeferredSeries.diff`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredSeries.diff),
[`DeferredDataFrame.interpolate`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.frames.html#apache_beam.dataframe.frames.DeferredDataFrame.interpolate)

Beam PCollections are inherently unordered, so pandas operations that are sensitive to the ordering of rows are not supported. These operations raise a [WontImplementError](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.dataframe.frame_base.html#apache_beam.dataframe.frame_base.WontImplementError).

Order-sensitive operations may be supported in the future. To track progress on this issue, follow [Issue 20862](https://github.com/apache/beam/issues/20862). If you think we should prioritize this work you can also [contact us](/community/contact-us/) to let us know.

**Workaround**

If you’re using [Interactive Beam](https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.runners.interactive.interactive_beam.html), you can use `collect` to bring a dataset into local memory and then perform these operations.

Alternatively, there may be ways to rewrite your code so that it’s not order sensitive. For example, pandas users often call the order-sensitive [`head`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.head.html) operation to peek at data, but if you just want to view a subset of elements, you can also use [`sample`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html), which doesn’t require you to collect the data first. Similarly, you could use `nlargest` instead of `sort_values(...).`.

### Operations that produce deferred scalars

Some DataFrame operations produce deferred scalars. In Beam, actual computation of the values is deferred, and so the values are not available for control flow. For example, you can compute a sum with `Series.sum`, but you can’t immediately branch on the result, because the result data is not immediately available. `Series.is_unique` is a similar example. Using a deferred scalar for branching logic or truth tests raises a [TypeError](https://github.com/apache/beam/blob/b908f595101ff4f21439f5432514005394163570/sdks/python/apache_beam/dataframe/frame_base.py#L117).

### Operations that aren’t implemented yet

The Beam DataFrame API implements many of the commonly used pandas DataFrame operations, and we’re actively working to support the remaining operations. But pandas has a large API, and there are still gaps ([Issue 20318](https://github.com/apache/beam/issues/20318)). If you invoke an operation that hasn’t been implemented yet, it will raise a `NotImplementedError`. Please [let us know](/community/contact-us/) if you encounter a missing operation that you think should be prioritized.

## Using Interactive Beam to access the full pandas API

Interactive Beam is a module designed for use in interactive notebooks. The module, which by convention is imported as `ib`, provides an `ib.collect` function that brings a `PCollection` or deferred DataFrame into local memory as a pandas DataFrame. After using `ib.collect` to materialize a deferred DataFrame you will be able to perform any operation in the pandas API, not just those that are supported in Beam.

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/dataframes.ipynb" >}}

To get started with Beam in a notebook, see [Try Apache Beam](/get-started/try-apache-beam/).
