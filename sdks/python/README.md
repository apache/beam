<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Apache Beam - Python SDK

[Apache Beam](http://beam.apache.org) is a unified model for defining both batch and streaming data-parallel processing pipelines. Beam provides a set of language-specific SDKs for constructing pipelines. These pipelines can be executed on distributed processing backends like [Apache Spark](http://spark.apache.org/), [Apache Flink](http://flink.apache.org), and [Google Cloud Dataflow](http://cloud.google.com/dataflow).

Apache Beam for Python provides access to Beam capabilities from the Python programming language.

## Table of Contents
  * [Overview of the Beam Programming Model](#overview-of-the-programming-model)
  * [Getting Started](#getting-started)
  * [A Quick Tour of the Source Code](#a-quick-tour-of-the-source-code)
  * [Simple Examples](#simple-examples)
      * [Basic pipeline](#basic-pipeline)
      * [Basic pipeline (with Map)](#basic-pipeline-with-map)
      * [Basic pipeline (with FlatMap)](#basic-pipeline-with-flatmap)
      * [Basic pipeline (with FlatMap and yield)](#basic-pipeline-with-flatmap-and-yield)
      * [Counting words](#counting-words)
      * [Counting words with GroupByKey](#counting-words-with-groupbykey)
      * [Type hints](#type-hints)
      * [BigQuery](#bigquery)
      * [Combiner examples](#combiner-examples)
  * [Organizing Your Code](#organizing-your-code)
  * [Contact Us](#contact-us)

## Overview of the Programming Model

The key concepts of the programming model are:

* PCollection - represents a collection of data, which could be bounded or unbounded in size.
* PTransform - represents a computation that transforms input PCollections into output
PCollections.
* Pipeline - manages a directed acyclic graph of PTransforms and PCollections that is ready
for execution.
* Runner - specifies where and how the Pipeline should execute.

For a further, detailed introduction, please read the
[Beam Programming Model](http://beam.apache.org/documentation/programming-guide).

## Getting Started

See [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/).

## A Quick Tour of the Source Code

With your virtual environment active, you can follow along this tour by running a `pydoc` server on a local port of your choosing (this example uses port 8888):

```
pydoc -p 8888
```

Open your browser and go to
http://localhost:8888/apache_beam.html

Some interesting classes to navigate to:

* `PCollection`, in file
[`apache_beam/pvalue.py`](http://localhost:8888/apache_beam.pvalue.html)
* `PTransform`, in file
[`apache_beam/transforms/ptransform.py`](http://localhost:8888/apache_beamtransforms.ptransform.html)
* `FlatMap`, `GroupByKey`, and `Map`, in file
[`apache_beam/transforms/core.py`](http://localhost:8888/apache_beam.transforms.core.html)
* combiners, in file
[`apache_beam/transforms/combiners.py`](http://localhost:8888/apache_beam.transforms.combiners.html)

Make sure you installed the package first. If not, run `python setup.py install`, then run pydoc with `pydoc -p 8888`.

## Simple Examples

The following examples demonstrate some basic, fundamental concepts for using Apache Beam's Python SDK. For more detailed examples, Beam provides a [directory of examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples) for Python.

### Basic pipeline

A basic pipeline will take as input an iterable, apply the
beam.Create `PTransform`, and produce a `PCollection` that can
be written to a file or modified by further `PTransform`s.
The `>>` operator is used to label `PTransform`s and
the `|` operator is used to chain them.

```python
# Standard imports
import apache_beam as beam
# Create a pipeline executing on a direct runner (local, non-cloud).
p = beam.Pipeline('DirectRunner')
# Create a PCollection with names and write it to a file.
(p
 | 'add names' >> beam.Create(['Ann', 'Joe'])
 | 'save' >> beam.io.WriteToText('./names'))
# Execute the pipeline.
p.run()
```

### Basic pipeline (with Map)

The `Map` `PTransform` returns one output per input. It takes a callable that is applied to each element of the input `PCollection` and returns an element to the output `PCollection`.

```python
import apache_beam as beam
p = beam.Pipeline('DirectRunner')
# Read a file containing names, add a greeting to each name, and write to a file.
(p
 | 'load names' >> beam.io.ReadFromText('./names')
 | 'add greeting' >> beam.Map(lambda name, msg: '%s, %s!' % (msg, name), 'Hello')
 | 'save' >> beam.io.WriteToText('./greetings'))
p.run()
```

### Basic pipeline (with FlatMap)

A `FlatMap` is like a `Map` except its callable returns a (possibly
empty) iterable of elements for the output `PCollection`.

The `FlatMap` transform returns zero to many output per input. It accepts a callable that is applied to each element of the input `PCollection` and returns an iterable with zero or more elements to the output `PCollection`.

```python
import apache_beam as beam
p = beam.Pipeline('DirectRunner')
# Read a file containing names, add two greetings to each name, and write to a file.
(p
 | 'load names' >> beam.io.ReadFromText('./names')
 | 'add greetings' >> beam.FlatMap(
    lambda name, messages: ['%s %s!' % (msg, name) for msg in messages],
    ['Hello', 'Hola'])
 | 'save' >> beam.io.WriteToText('./greetings'))
p.run()
```

### Basic pipeline (with FlatMap and yield)

The callable of a `FlatMap` can be a generator, that is,
a function using `yield`.

```python
import apache_beam as beam
p = beam.Pipeline('DirectRunner')
# Read a file containing names, add two greetings to each name
# (with FlatMap using a yield generator), and write to a file.
def add_greetings(name, messages):
  for msg in messages:
    yield '%s %s!' % (msg, name)

(p
 | 'load names' >> beam.io.ReadFromText('./names')
 | 'add greetings' >> beam.FlatMap(add_greetings, ['Hello', 'Hola'])
 | 'save' >> beam.io.WriteToText('./greetings'))
p.run()
```

### Counting words

This example shows how to read a text file from [Google Cloud Storage](https://cloud.google.com/storage/) and count its words.

```python
import re
import apache_beam as beam
p = beam.Pipeline('DirectRunner')
(p
 | 'read' >> beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
 | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
 | 'count words' >> beam.combiners.Count.PerElement()
 | 'save' >> beam.io.WriteToText('./word_count'))
p.run()
```

### Counting words with GroupByKey

This is a somewhat forced example of `GroupByKey` to count words as the previous example did, but without using `beam.combiners.Count.PerElement`. As shown in the example, you can use a wildcard to specify the text file source.

```python
import re
import apache_beam as beam
p = beam.Pipeline('DirectRunner')
class MyCountTransform(beam.PTransform):
  def expand(self, pcoll):
    return (pcoll
            | 'one word' >> beam.Map(lambda word: (word, 1))
            # GroupByKey accepts a PCollection of (word, 1) elements and
            # outputs a PCollection of (word, [1, 1, ...])
            | 'group words' >> beam.GroupByKey()
            | 'count words' >> beam.Map(lambda (word, counts): (word, len(counts))))

(p
 | 'read' >> beam.io.ReadFromText('./names*')
 | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
 | MyCountTransform()
 | 'write' >> beam.io.WriteToText('./word_count'))
p.run()
```

### Type hints

In some cases, providing type hints can improve the efficiency
of the data encoding.

```python
import apache_beam as beam
from apache_beam.typehints import typehints
p = beam.Pipeline('DirectRunner')
(p
 | 'read' >> beam.io.ReadFromText('./names')
 | 'add types' >> beam.Map(lambda x: (x, 1)).with_output_types(typehints.KV[str, int])
 | 'group words' >> beam.GroupByKey()
 | 'save' >> beam.io.WriteToText('./typed_names'))
p.run()
```

### BigQuery

This example reads weather data from a BigQuery table, calculates the number of tornadoes per month, and writes the results to a table you specify.

```python
import apache_beam as beam
project = 'DESTINATION-PROJECT-ID'
input_table = 'clouddataflow-readonly:samples.weather_stations'
output_table = 'DESTINATION-DATASET.DESTINATION-TABLE'

p = beam.Pipeline(argv=['--project', project])
(p
 | 'read' >> beam.Read(beam.io.BigQuerySource(input_table))
 | 'months with tornadoes' >> beam.FlatMap(
    lambda row: [(int(row['month']), 1)] if row['tornado'] else [])
 | 'monthly count' >> beam.CombinePerKey(sum)
 | 'format' >> beam.Map(lambda (k, v): {'month': k, 'tornado_count': v})
 | 'save' >> beam.Write(
    beam.io.BigQuerySink(
        output_table,
        schema='month:INTEGER, tornado_count:INTEGER',
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
p.run()
```

This pipeline, like the one above, calculates the number of tornadoes per month, but it uses a query to filter out the input instead of using the whole table.

```python
import apache_beam as beam
project = 'DESTINATION-PROJECT-ID'
output_table = 'DESTINATION-DATASET.DESTINATION-TABLE'
input_query = 'SELECT month, COUNT(month) AS tornado_count ' \
        'FROM [clouddataflow-readonly:samples.weather_stations] ' \
        'WHERE tornado=true GROUP BY month'
p = beam.Pipeline(argv=['--project', project])
(p
 | 'read' >> beam.Read(beam.io.BigQuerySource(query=input_query))
 | 'save' >> beam.Write(beam.io.BigQuerySink(
    output_table,
    schema='month:INTEGER, tornado_count:INTEGER',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
p.run()
```

### Combiner Examples

Combiner transforms use "reducing" functions, such as sum, min, or max, to combine multiple values of a `PCollection` into a single value.

```python
import apache_beam as beam
p = beam.Pipeline('DirectRunner')

SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20)]

(p
 | beam.Create(SAMPLE_DATA)
 | beam.CombinePerKey(sum)
 | beam.io.WriteToText('./sums'))
p.run()
```

The [combiners_test.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/combiners_test.py) file contains more combiner examples.

## Organizing Your Code

Many projects will grow to multiple source code files. It is recommended that you organize your project so that all code involved in running your pipeline can be built as a Python package. This way, the package can easily be installed in the VM workers executing the job.

Follow the [Juliaset example](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete/juliaset). If the code is organized in this fashion, you can use the `--setup_file` command line option to create a source distribution out of the project files, stage the resulting tarball, and later install it in the workers executing the job.

## More Information

Please report any issues on [JIRA](https://issues.apache.org/jira/browse/BEAM/component/12328910).

If you’re interested in contributing to the Beam SDK, start by reading the [Contribute](http://beam.apache.org/contribute/) guide.
