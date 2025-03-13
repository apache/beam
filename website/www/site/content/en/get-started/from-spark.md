---
title: "Getting started from Apache Spark"
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

# Getting started from Apache Spark

{{< localstorage language language-py >}}

If you already know [_Apache Spark_](https://spark.apache.org/),
using Beam should be easy.
The basic concepts are the same, and the APIs are similar as well.

Spark stores data in _Spark DataFrames_ for structured data,
and in _Resilient Distributed Datasets_ (RDD) for unstructured data.
We are using RDDs for this guide.

A Spark RDD represents a collection of elements,
while in Beam it's called a _Parallel Collection_ (PCollection).
A PCollection in Beam does _not_ have any ordering guarantees.

Likewise, a transform in Beam is called a _Parallel Transform_ (PTransform).

Here are some examples of common operations and their equivalent between PySpark and Beam.

## Overview

Here's a simple example of a PySpark pipeline that takes the numbers from one to four,
multiplies them by two, adds all the values together, and prints the result.

{{< highlight py >}}
import pyspark

sc = pyspark.SparkContext()
result = (
    sc.parallelize([1, 2, 3, 4])
    .map(lambda x: x * 2)
    .reduce(lambda x, y: x + y)
)
print(result)
{{< /highlight >}}

In Beam you pipe your data through the pipeline using the
_pipe operator_ `|` like `data | beam.Map(...)` instead of chaining
methods like `data.map(...)`, but they're doing the same thing.

Here's what an equivalent pipeline looks like in Beam.

{{< highlight py >}}
import apache_beam as beam

with beam.Pipeline() as pipeline:
    result = (
        pipeline
        | beam.Create([1, 2, 3, 4])
        | beam.Map(lambda x: x * 2)
        | beam.CombineGlobally(sum)
        | beam.Map(print)
    )
{{< /highlight >}}

> ℹ️ Note that we called `print` inside a `Map` transform.
> That's because we can only access the elements of a PCollection
> from within a PTransform.
> To inspect the data locally, you can use the [InteractiveRunner](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development#creating_your_pipeline)

Another thing to note is that Beam pipelines are constructed lazily.
This means that when you pipe `|` data you're only declaring the
transformations and the order you want them to happen,
but the actual computation doesn't happen.
The pipeline is run after the `with beam.Pipeline() as pipeline` context has
closed.

> ℹ️ When the `with beam.Pipeline() as pipeline` context closes,
> it implicitly calls `pipeline.run()` which triggers the computation to happen.

The pipeline is then sent to your
[runner of choice](/documentation/runners/capability-matrix/)
and it processes the data.

> ℹ️ The pipeline can run locally with the _DirectRunner_,
> or in a distributed runner such as Flink, Spark, or Dataflow.
> The Spark runner is not related to PySpark.

A label can optionally be added to a transform using the
_right shift operator_ `>>` like `data | 'My description' >> beam.Map(...)`.
This serves both as comments and makes your pipeline easier to debug.

This is how the pipeline looks after adding labels.

{{< highlight py >}}
import apache_beam as beam

with beam.Pipeline() as pipeline:
    result = (
        pipeline
        | 'Create numbers' >> beam.Create([1, 2, 3, 4])
        | 'Multiply by two' >> beam.Map(lambda x: x * 2)
        | 'Sum everything' >> beam.CombineGlobally(sum)
        | 'Print results' >> beam.Map(print)
    )
{{< /highlight >}}

## Setup

Here's a comparison on how to get started both in PySpark and Beam.

<div class="table-container-wrapper">
{{< table >}}
<table style="width:100%" class="table-wrapper--equal-p">
<tr>
    <th style="width:20%"></th>
    <th style="width:40%">PySpark</th>
    <th style="width:40%">Beam</th>
</tr>
<tr>
    <td><b>Install</b></td>
    <td><code>$ pip install pyspark</code></td>
    <td><code>$ pip install apache-beam</code></td>
</tr>
<tr>
    <td><b>Imports</b></td>
    <td><code>import pyspark</code></td>
    <td><code>import apache_beam as beam</code></td>
</tr>
<tr>
    <td><b>Creating a<br>local pipeline</b></td>
    <td>
        <code>sc = pyspark.SparkContext() as sc:</code><br>
        <code># Your pipeline code here.</code>
    </td>
    <td>
        <code>with beam.Pipeline() as pipeline:</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;# Your pipeline code here.</code>
    </td>
</tr>
<tr>
    <td><b>Creating values</b></td>
    <td><code>values = sc.parallelize([1, 2, 3, 4])</code></td>
    <td><code>values = pipeline | beam.Create([1, 2, 3, 4])</code></td>
</tr>
<tr>
    <td><b>Creating<br>key-value pairs</b></td>
    <td>
        <code>pairs = sc.parallelize([</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;('key1', 'value1'),</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;('key2', 'value2'),</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;('key3', 'value3'),</code><br>
        <code>])</code>
    </td>
    <td>
        <code>pairs = pipeline | beam.Create([</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;('key1', 'value1'),</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;('key2', 'value2'),</code><br>
        <code>&nbsp;&nbsp;&nbsp;&nbsp;('key3', 'value3'),</code><br>
        <code>])</code>
    </td>
</tr>
<tr>
    <td><b>Running a<br>local pipeline</b></td>
    <td><code>$ spark-submit spark_pipeline.py</code></td>
    <td><code>$ python beam_pipeline.py</code></td>
</tr>
</table>
{{< /table >}}
</div>

## Transforms

Here are the equivalents of some common transforms in both PySpark and Beam.

<div class="table-container-wrapper">
{{< table >}}
<table style="width:100%" class="table-wrapper--equal-p">
<tr>
    <th style="width:20%"></th>
    <th style="width:40%">PySpark</th>
    <th style="width:40%">Beam</th>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/elementwise/map/">Map</a></b></td>
    <td><code>values.map(lambda x: x * 2)</code></td>
    <td><code>values | beam.Map(lambda x: x * 2)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/elementwise/filter/">Filter</a></b></td>
    <td><code>values.filter(lambda x: x % 2 == 0)</code></td>
    <td><code>values | beam.Filter(lambda x: x % 2 == 0)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/elementwise/flatmap/">FlatMap</a></b></td>
    <td><code>values.flatMap(lambda x: range(x))</code></td>
    <td><code>values | beam.FlatMap(lambda x: range(x))</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/groupbykey/">Group by key</a></b></td>
    <td><code>pairs.groupByKey()</code></td>
    <td><code>pairs | beam.GroupByKey()</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/combineglobally/">Reduce</a></b></td>
    <td><code>values.reduce(lambda x, y: x+y)</code></td>
    <td><code>values | beam.CombineGlobally(sum)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/combineperkey/">Reduce by key</a></b></td>
    <td><code>pairs.reduceByKey(lambda x, y: x+y)</code></td>
    <td><code>pairs | beam.CombinePerKey(sum)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/distinct/">Distinct</a></b></td>
    <td><code>values.distinct()</code></td>
    <td><code>values | beam.Distinct()</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/count/">Count</a></b></td>
    <td><code>values.count()</code></td>
    <td><code>values | beam.combiners.Count.Globally()</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/count/">Count by key</a></b></td>
    <td><code>pairs.countByKey()</code></td>
    <td><code>pairs | beam.combiners.Count.PerKey()</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/top/">Take smallest</a></b></td>
    <td><code>values.takeOrdered(3)</code></td>
    <td><code>values | beam.combiners.Top.Smallest(3)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/top/">Take largest</a></b></td>
    <td><code>values.takeOrdered(3, lambda x: -x)</code></td>
    <td><code>values | beam.combiners.Top.Largest(3)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/sample/">Random sample</a></b></td>
    <td><code>values.takeSample(False, 3)</code></td>
    <td><code>values | beam.combiners.Sample.FixedSizeGlobally(3)</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/other/flatten/">Union</a></b></td>
    <td><code>values.union(otherValues)</code></td>
    <td><code>(values, otherValues) | beam.Flatten()</code></td>
</tr>
<tr>
    <td><b><a href="/documentation/transforms/python/aggregation/cogroupbykey/">Co-group</a></b></td>
    <td><code>pairs.cogroup(otherPairs)</code></td>
    <td><code>{'Xs': pairs, 'Ys': otherPairs} | beam.CoGroupByKey()</code></td>
</tr>
</table>
{{< /table >}}
</div>

> ℹ️ To learn more about the transforms available in Beam, check the
> [Python transform gallery](/documentation/transforms/python/overview).

## Using calculated values

Since we are working in potentially distributed environments,
we can't guarantee that the results we've calculated are available at any given machine.

In PySpark, we can get a result from a collection of elements (RDD) by using
`data.collect()`, or other aggregations such as `reduce()`, `count()`, and more.

Here's an example to scale numbers into a range between zero and one.

{{< highlight py >}}
import pyspark

sc = pyspark.SparkContext()
values = sc.parallelize([1, 2, 3, 4])
min_value = values.reduce(min)
max_value = values.reduce(max)

# We can simply use `min_value` and `max_value` since it's already a Python `int` value from `reduce`.
scaled_values = values.map(lambda x: (x - min_value) / (max_value - min_value))

# But to access `scaled_values`, we need to call `collect`.
print(scaled_values.collect())
{{< /highlight >}}

In Beam the results from all transforms result in a PCollection.
We use [_side inputs_](/documentation/programming-guide/#side-inputs)
to feed a PCollection into a transform and access its values.

Any transform that accepts a function, like
[`Map`](/documentation/transforms/python/elementwise/map),
can take side inputs.
If we only need a single value, we can use
[`beam.pvalue.AsSingleton`](https://beam.apache.org/releases/pydoc/current/apache_beam.pvalue.html#apache_beam.pvalue.AsSingleton) and access them as a Python value.
If we need multiple values, we can use
[`beam.pvalue.AsIter`](https://beam.apache.org/releases/pydoc/current/apache_beam.pvalue.html#apache_beam.pvalue.AsIter)
and access them as an [`iterable`](https://docs.python.org/3/glossary.html#term-iterable).

{{< highlight py >}}
import apache_beam as beam

with beam.Pipeline() as pipeline:
    values = pipeline | beam.Create([1, 2, 3, 4])
    min_value = values | beam.CombineGlobally(min)
    max_value = values | beam.CombineGlobally(max)

    # To access `min_value` and `max_value`, we need to pass them as a side input.
    scaled_values = values | beam.Map(
        lambda x, minimum, maximum: (x - minimum) / (maximum - minimum),
        minimum=beam.pvalue.AsSingleton(min_value),
        maximum=beam.pvalue.AsSingleton(max_value),
    )

    scaled_values | beam.Map(print)
{{< /highlight >}}

> ℹ️ In Beam we need to pass a side input explicitly, but we get the
> benefit that a reduction or aggregation does _not_ have to fit into memory.
> Lazily computing side inputs also allows us to compute `values` only once,
> rather than for each distinct reduction (or requiring explicit caching of the RDD).

## Next Steps

* Take a look at all the available transforms in the [Python transform gallery](/documentation/transforms/python/overview).
* Learn how to read from and write to files in the [_Pipeline I/O_ section of the _Programming guide_](/documentation/programming-guide/#pipeline-io)
* Walk through additional WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/get-started/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.
* If you're interested in contributing to the Apache Beam codebase, see the [Contribution Guide](/contribute).

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
