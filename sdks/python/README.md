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
== This page is currently being updated. ==

# Cloud Dataflow SDK for Python

[Google Cloud Dataflow](https://cloud.google.com/dataflow/)
provides a simple, powerful programming model for building both batch
and streaming parallel data processing pipelines.

The Dataflow SDK for Python provides access to Dataflow capabilities
from the Python programming language.

## Table of Contents
  * [Status of this Release](#status-of-this-release)
  * [Signing up for Alpha Batch Cloud Execution](#signing-up-for-alpha-batch-cloud-execution)
  * [Overview of Dataflow Programming](#overview-of-dataflow-programming)
  * [Getting Started](#getting-started)
      * [Setting up an environment](#setting-up-an-environment)
          * [Install ``pip``](#install-pip)
          * [Install ``virtualenv``](#install-virtualenv)
          * [Install ``setuptools``](#install-setuptools)
      * [Getting the Dataflow software](#getting-the-dataflow-software)
          * [Create and activate virtual environment](#create-and-activate-virtual-environment)
          * [Download and install](#download-and-install)
          * [Notes on installing with ``setup.py install``](#notes-on-installing-with-setuppy-install)
  * [Local execution of a pipeline](#local-execution-of-a-pipeline)
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
      * [Combiner Examples](#combiner-examples)
      * [More Examples](#more-examples)
  * [Organizing Your Code](#organizing-your-code)
  * [Contact Us](#contact-us)

## Status of this Release

This is a version of Google Cloud Dataflow SDK for Python that is
still early in its development, and significant changes
should be expected before the first stable version.

Google recently
[announced its intention](http://googlecloudplatform.blogspot.com/2016/01/Dataflow-and-open-source-proposal-to-join-the-Apache-Incubator.html)
to donate the Google Cloud Dataflow SDKs and Programming Model to
the Apache Software Foundation (ASF), after which they will be called the
Apache Beam SDKs.

The SDK for Java is actively transitioning to
[Apache Beam](http://beam.incubator.apache.org/),
an ASF incubator project.  The SDK for Python will be added
to Apache Beam soon after.  Expect many renames.

## Signing up for Alpha Batch Cloud Execution

Google Cloud Dataflow now provides Alpha support for Batch pipelines written
with the SDK for Python. This Alpha program is designed to give customers access
to the service for early testing. Customers are advised
not to use this feature in production systems. If you are interested in
being considered to participate in the Alpha program,
please submit this [form](http://goo.gl/forms/o4w14whz9x).
Note that filling the form does not guarantee entry to the Alpha program.

## Overview of Dataflow Programming

For an introduction to the programming model, please read
[Dataflow Programming Model](https://cloud.google.com/dataflow/model/programming-model)
but note that some examples on that site use only Java.
The key concepts of the programming model are

* [`PCollection`](https://cloud.google.com/dataflow/model/pcollection):
represents a collection of data, which could be bounded or unbounded in size.
* [`PTransform`](https://cloud.google.com/dataflow/model/transforms):
represents a computation that transforms input PCollections into output
PCollections.
* [`Pipeline`](https://cloud.google.com/dataflow/model/pipelines):
manages a directed acyclic graph of PTransforms and PCollections that is ready
for execution.
* `Runner`:
specifies where and how the Pipeline should execute.

This release has some significant limitations:

* We provide only one PipelineRunner, the `DirectPipelineRunner`.
* The Google Cloud Dataflow service does not yet accept jobs from this SDK.
* Triggers are not supported.
* The SDK works only on Python 2.7.

## Getting Started

### Setting up an environment

If this is the first time you are installing the Dataflow SDK, you may need to
set up your machine's Python development environment.

#### Install ``pip``

`pip` is Python's package manager.  If you already have `pip` installed
(type `pip -V` to check), skip this step.

There are several ways to install `pip`; use whichever works for you.

Preferred option: install using your system's package manager, which may be
*one* of the following commands, depending on your Linux distribution:

```sh
    sudo yum install python-pip
    sudo apt-get install python-pip
    sudo zypper install python-pip
```

Otherwise, if you have `easy_install` (likely if you are on MacOS):

    sudo easy_install pip

Or you may have to install the bootstrapper.  Download the following script
to your system: https://bootstrap.pypa.io/get-pip.py
You can fetch it with your browser or use a command-line program, such as *one*
of the following:

```sh
    curl -O https://bootstrap.pypa.io/get-pip.py
    wget https://bootstrap.pypa.io/get-pip.py
```

After downloading `get-pip.py`, run it to install `pip`:

```sh
python ./get-pip.py
```

#### Install ``virtualenv``

We recommend installing in a
[Python virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
for initial experiments.  If you do not have `virtualenv` version 13.1.0
or later (type `virtualenv --version` to check), it will install a too-old
version of `setuptools` in the virtual environment.  To install (or upgrade)
your `virtualenv`:

    pip install --upgrade virtualenv

#### Install ``setuptools``

If you are not going to use a Python virtual environment (but we recommend you
do; see the previous section), ensure `setuptools` version 17.1 or newer is
installed (type `easy_install --version` to check).  If you do not have that
installed:

    pip install --upgrade setuptools

### Getting the Dataflow software

#### Create and activate virtual environment

A virtual environment is a directory tree containing its own Python
distribution.  To create a virtual environment:

    virtualenv /path/to/directory

A virtual environment needs to be activated for each shell that is to use it;
activating sets some environment variables that point to the virtual
environment's directories.  To activate a virtual environment in Bash:

    . /path/to/directory/bin/activate

That is, source the script `bin/activate` under the virtual environment
directory you created.

#### Download and install

Install the latest tarball from GitHub by browsing to
<https://github.com/GoogleCloudPlatform/DataflowPythonSDK/releases/latest>
and copying one of the "Source code" links.  The `.tar.gz` file is smaller;
we'll assume you use that one.  With a virtual environment active, paste the
URL into a ``pip install`` shell command, executing something like this:

```sh
pip install https://github.com/GoogleCloudPlatform/DataflowPythonSDK/archive/vX.Y.Z.tar.gz
```

#### Notes on installing with ``setup.py install``

We recommend installing using ``pip install``, as described above.
However, you also may install from an unpacked source code tree.
You can get such a tree by un-tarring the ``.tar.gz`` file or
by using ``git clone``.  From a source tree, you can install by running

    cd DataflowPythonSDK*
    python setup.py install --root /
    python setup.py test

The ``--root /`` prevents Dataflow from being installed as an ``egg`` package.
This workaround prevents failures if Dataflow is installed in the same virtual
environment as another package under the ``google`` top-level package.

If you get import errors during or after installing with ``setup.py``,
uninstall the package:

    pip uninstall python-dataflow

and use the ``pip install`` method described above to re-install it.

## Local execution of a pipeline

The `$VIRTUAL_ENV/lib/python2.7/site-packages/google/cloud/dataflow/examples`
subdirectory (the `google/cloud/dataflow/examples` subdirectory in the
source distribution) has many examples large and small.

All examples can be run locally by passing the arguments required by the
example script. For instance, to run `wordcount.py`, try:

    python -m google.cloud.dataflow.examples.wordcount --output OUTPUT_FILE

## A Quick Tour of the Source Code

You can follow along this tour by, with your virtual environment
active, running a `pydoc` server on a local port of your choosing
(this example uses port 8888).

    pydoc -p 8888

Now open your browser and go to
http://localhost:8888/google.cloud.dataflow.html

Some interesting classes to navigate to:

* `PCollection`, in file
[`google/cloud/dataflow/pvalue.py`](http://localhost:8888/google.cloud.dataflow.pvalue.html)
* `PTransform`, in file
[`google/cloud/dataflow/transforms/ptransform.py`](http://localhost:8888/google.cloud.dataflow.transforms.ptransform.html)
* `FlatMap`, `GroupByKey`, and `Map`, in file
[`google/cloud/dataflow/transforms/core.py`](http://localhost:8888/google.cloud.dataflow.transforms.core.html)
* combiners, in file
[`google/cloud/dataflow/transforms/combiners.py`](http://localhost:8888/google.cloud.dataflow.transforms.combiners.html)

## Simple Examples

### Basic pipeline

A basic pipeline creates a `PCollection` from an iterable
and uses the pipe operator to chain `PTransform`s.

```python
# Standard imports
import apache_beam as beam
# Create a pipeline executing on a direct runner (local, non-cloud).
p = beam.Pipeline('DirectPipelineRunner')
# Create a PCollection with names and write it to a file.
(p
 | 'add names' >> beam.Create(['Ann', 'Joe'])
 | 'save' >> beam.io.Write(beam.io.TextFileSink('./names')))
# Execute the pipeline.
p.run()
```

### Basic pipeline (with Map)

The `Map` `PTransform` takes a callable, which will be applied to each
element of the input `PCollection` and must return an element to go
into the output `PCollection`.

```python
import apache_beam as beam
p = beam.Pipeline('DirectPipelineRunner')
# Read a file containing names, add a greeting to each name, and write to a file.
(p
 | 'load names' >> beam.Read(beam.io.TextFileSource('./names'))
 | 'add greeting' >> beam.Map(lambda name, msg: '%s, %s!' % (msg, name), 'Hello')
 | 'save' >> beam.Write(beam.io.TextFileSink('./greetings')))
p.run()
```

### Basic pipeline (with FlatMap)

A `FlatMap` is like a `Map` except its callable returns a (possibly
empty) iterable of elements for the output `PCollection`.

```python
import apache_beam as beam
p = beam.Pipeline('DirectPipelineRunner')
# Read a file containing names, add two greetings to each name, and write to a file.
(p
 | 'load names' >> beam.Read(beam.io.TextFileSource('./names'))
 | 'add greetings' >> beam.FlatMap(
    lambda name, messages: ['%s %s!' % (msg, name) for msg in messages],
    ['Hello', 'Hola'])
 | 'save' >> beam.Write(beam.io.TextFileSink('./greetings')))
p.run()
```

### Basic pipeline (with FlatMap and yield)

The callable of a `FlatMap` can be a generator, that is,
a function using `yield`.

```python
import apache_beam as beam
p = beam.Pipeline('DirectPipelineRunner')
# Read a file containing names, add two greetings to each name
# (with FlatMap using a yield generator), and write to a file.
def add_greetings(name, messages):
  for msg in messages:
    yield '%s %s!' % (msg, name)

(p
 | 'load names' >> beam.Read(beam.io.TextFileSource('./names'))
 | 'add greetings' >> beam.FlatMap(add_greetings, ['Hello', 'Hola'])
 | 'save' >> beam.Write(beam.io.TextFileSink('./greetings')))
p.run()
```

### Counting words

This example shows how to read a text file from
[Google Cloud Storage](https://cloud.google.com/storage/)
and count its words.

```python
import re
import apache_beam as beam
p = beam.Pipeline('DirectPipelineRunner')
(p
 | 'read' >> beam.Read(
    beam.io.TextFileSource('gs://dataflow-samples/shakespeare/kinglear.txt'))
 | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
 | 'count words' >> beam.combiners.Count.PerElement()
 | 'save' >> beam.Write(beam.io.TextFileSink('./word_count')))
p.run()
```

### Counting words with GroupByKey

This is a somewhat forced example of `GroupByKey` to achieve the same
functionality of the previous example without using
`beam.combiners.Count.PerElement`. It demonstrates also the use of a
wildcard to specify the text file source.
```python
import re
import apache_beam as beam
p = beam.Pipeline('DirectPipelineRunner')
class MyCountTransform(beam.PTransform):
  def apply(self, pcoll):
    return (pcoll
            | 'one word' >> beam.Map(lambda word: (word, 1))
            # GroupByKey accepts a PCollection of (word, 1) elements and
            # outputs a PCollection of (word, [1, 1, ...])
            | 'group words' >> beam.GroupByKey()
            | 'count words' >> beam.Map(lambda (word, counts): (word, len(counts))))

(p
 | 'read' >> beam.Read(beam.io.TextFileSource('./names*'))
 | 'split' >> beam.FlatMap(lambda x: re.findall(r'\w+', x))
 | MyCountTransform()
 | 'write' >> beam.Write(beam.io.TextFileSink('./word_count')))
p.run()
```

### Type hints

In some cases, providing type hints can improve the efficiency
of the data encoding.

```python
import apache_beam as beam
from apache_beam.typehints import typehints
p = beam.Pipeline('DirectPipelineRunner')
(p
 | 'read' >> beam.Read(beam.io.TextFileSource('./names'))
 | 'add types' >> beam.Map(lambda x: (x, 1)).with_output_types(typehints.KV[unicode, int])
 | 'group words' >> beam.GroupByKey()
 | 'save' >> beam.Write(beam.io.TextFileSink('./typed_names')))
p.run()
```

### BigQuery

This example calculates the number of tornadoes per month (from weather data).
The input is read from a BigQuery table and the output is written to a
different table specified by the user, along with a target project.

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

This pipeline calculates the number of tornadoes per month, but it uses
a query to filter out the input instead of using the whole table.

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

Combiners are used to create a `PCollection` that contains the sums
(or max or min) of each of the keys in the initial `PCollecion`.
Such standard Python functions can be used directly as combiner
functions. In fact, any function "reducing" an iterable to a
single value can be used.

```python
import apache_beam as beam
p = beam.Pipeline('DirectPipelineRunner')

SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20)]

(p
 | beam.Create(SAMPLE_DATA)
 | beam.CombinePerKey(sum)
 | beam.Write(beam.io.TextFileSink('./sums')))
p.run()
```

The `google/cloud/dataflow/examples/cookbook/combiners_test.py` file in the
source distribution contains more combiner examples.

### More Examples

The `google/cloud/dataflow/examples` subdirectory in the
source distribution has some larger examples.

## Organizing Your Code

Many projects will grow to multiple source code files. It is beneficial to
organize the project so that all the code involved in running a workflow can be
built as a Python package so that it can be installed in the VM workers
executing a job.

Please follow the example in `google/cloud/dataflow/examples/complete/juliaset`.
If the code is organized in this fashion then you can use the `--setup_file`
command line option to create a source distribution out of the project files,
stage the resulting tarball and later install it in the workers executing the
job.

## Contact Us

We welcome all usage-related questions on
[Stack Overflow](https://stackoverflow.com/questions/tagged/google-cloud-dataflow)
tagged with `google-cloud-dataflow`.

Please use the
[issue tracker](https://github.com/GoogleCloudPlatform/DataflowPythonSDK/issues)
on GitHub to report any bugs, comments or questions regarding SDK development.
