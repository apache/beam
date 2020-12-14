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

If you already know [_Apache Spark_](http://spark.apache.org/),
learning _Apache Beam_ is familiar.
The Beam and Spark APIs are similar, so you already know the basic concepts.

Spark stores data _Spark DataFrames_ for structured data,
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

Another thing to note is that Beam pipelines are constructed lazily.
This means that when you pipe `|` data you're only declaring the
transformations and the order you want them to happen,
but the actual computation doesn't happen.
The pipeline is run after the `with beam.Pipeline() as pipeline` context has
closed.

> ℹ️ When the `with beam.Pipeline() as pipeline` context closes,
> it implicitly calls `pipeline.run()` which triggers the computation to happen.

The pipeline is then sent to your
[runner of choice](https://beam.apache.org/documentation/runners/capability-matrix/)
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

{{< table >}}
<table>
<tr>
    <th></th>
    <th>PySpark</th>
    <th>Beam</th>
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

## Transforms

Here are the equivalents of some common transforms in both PySpark and Beam.

{{< table >}}
|                                                                                  | PySpark                               | Beam                                                    |
|----------------------------------------------------------------------------------|---------------------------------------|---------------------------------------------------------|
| [**Map**](/documentation/transforms/python/elementwise/map/)                     | `values.map(lambda x: x * 2)`         | `values | beam.Map(lambda x: x * 2)`                    |
| [**Filter**](/documentation/transforms/python/elementwise/filter/)               | `values.filter(lambda x: x % 2 == 0)` | `values | beam.Filter(lambda x: x % 2 == 0)`            |
| [**FlatMap**](/documentation/transforms/python/elementwise/flatmap/)             | `values.flatMap(lambda x: range(x))`  | `values | beam.FlatMap(lambda x: range(x))`             |
| [**Group by key**](/documentation/transforms/python/aggregation/groupbykey/)     | `pairs.groupByKey()`                  | `pairs | beam.GroupByKey()`                             |
| [**Reduce**](/documentation/transforms/python/aggregation/combineglobally/)      | `values.reduce(lambda x, y: x+y)`     | `values | beam.CombineGlobally(sum)`                    |
| [**Reduce by key**](/documentation/transforms/python/aggregation/combineperkey/) | `pairs.reduceByKey(lambda x, y: x+y)` | `pairs | beam.CombinePerKey(sum)`                       |
| [**Distinct**](/documentation/transforms/python/aggregation/distinct/)           | `values.distinct()`                   | `values | beam.Distinct()`                              |
| [**Count**](/documentation/transforms/python/aggregation/count/)                 | `values.count()`                      | `values | beam.combiners.Count.Globally()`              |
| [**Count by key**](/documentation/transforms/python/aggregation/count/)          | `pairs.countByKey()`                  | `pairs | beam.combiners.Count.PerKey()`                 |
| [**Take smallest**](/documentation/transforms/python/aggregation/top/)           | `values.takeOrdered(3)`               | `values | beam.combiners.Top.Smallest(3)`               |
| [**Take largest**](/documentation/transforms/python/aggregation/top/)            | `values.takeOrdered(3, lambda x: -x)` | `values | beam.combiners.Top.Largest(3)`                |
| [**Random sample**](/documentation/transforms/python/aggregation/sample/)        | `values.takeSample(False, 3)`         | `values | beam.combiners.Sample.FixedSizeGlobally(3)`   |
| [**Union**](/documentation/transforms/python/other/flatten/)                     | `values.union(otherValues)`           | `(values, otherValues) | beam.Flatten()`                |
| [**Co-group**](/documentation/transforms/python/aggregation/cogroupbykey/)       | `pairs.cogroup(otherPairs)`           | `{'Xs': pairs, 'Ys': otherPairs} | beam.CoGroupByKey()` |
{{< /table >}}

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
total = values.reduce(lambda x, y: x + y)

# We can simply use `total` since it's already a Python `int` value from `reduce`.
scaled_values = values.map(lambda x: x / total)

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
    total = values | beam.CombineGlobally(sum)

    # To access `total`, we need to pass it as a side input.
    scaled_values = values | beam.Map(
        lambda x, total: x / total,
        total=beam.pvalue.AsSingleton(total))

    scaled_values | beam.Map(print)
{{< /highlight >}}

> ℹ️ In Beam we need to pass a side input explicitly, but we get the
> benefit that a reduction or aggregation does _not_ have to fit into memory.

## Next Steps

* Take a look at all the available transforms in the [Python transform gallery](/documentation/transforms/python/overview).
* Learn how to read from and write to files in the [_Pipeline I/O_ section of the _Programming guide_](/documentation/programming-guide/#pipeline-io)
* Walk through additional WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.
* If you're interested in contributing to the Apache Beam codebase, see the [Contribution Guide](/contribute).

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
