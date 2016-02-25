# Cloud Dataflow SDK for Python

[Google Cloud Dataflow](https://cloud.google.com/dataflow/)
provides a simple, powerful programming model for building both batch
and streaming parallel data processing pipelines.

The Dataflow SDK for Python provides access to Dataflow capabilities
from the Python programming language.

## Status of this Release

This is the Google Cloud Dataflow SDK for Python version 0.2.0.
It is still early in its development, and significant changes
should be expected before the first stable version.

Google recently
[announced its desire](http://googlecloudplatform.blogspot.com/2016/01/Dataflow-and-open-source-proposal-to-join-the-Apache-Incubator.html)
to donate the Google Cloud Dataflow SDKs and Programming Model to
the Apache Software Foundation (ASF), after which they will be called the
Apache Beam SDKs.

The SDK for Java is actively transitioning to
[Apache Beam](http://beam.incubator.apache.org/),
an ASF incubator project.  The SDK for Python will be added
to Apache Beam soon after.  Expect many renames.

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
do; see the previous section), then you will need to ensure `setuptools`
version 17.1 or newer is installed on your system (type `easy_install
--version` to check).  If you do not have that installed:

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

#### Download

Clone the SDK from GitHub:

    git clone https://github.com/GoogleCloudPlatform/DataflowPythonSDK

#### Install

With a virtual environment active, install the Dataflow package:

    cd DataflowPythonSDK
    python setup.py install

#### Test

After install, run the tests to make sure everything is okay.

    python setup.py test

## Local execution of a pipeline

The `google/cloud/dataflow/examples` subdirectory in the
source distribution has many examples large and small.

All examples can be run locally by passing the arguments required by the
example script. For instance, to run `wordcount.py`, try:

    python google/cloud/dataflow/examples/wordcount.py --output OUTPUT_FILE

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

## Some Simple Examples

### Hello world

Create a transform from an iterable and use the pipe operator to chain
transforms:

```python
# Standard imports
import google.cloud.dataflow as df
# Create a pipeline executing on a direct runner (local, non-cloud).
p = df.Pipeline('DirectPipelineRunner')
# Create a PCollection with names and write it to a file.
(p
 | df.Create('add names', ['Ann', 'Joe'])
 | df.Write('save', df.io.TextFileSink('./names')))
# Execute the pipeline.
p.run()
```

### Hello world (with Map)

The `Map` transform takes a callable, which will be applied to each
element of the input `PCollection` and must return an element to go
into the output `PCollection`.

```python
import google.cloud.dataflow as df
p = df.Pipeline('DirectPipelineRunner')
# Read file with names, add a greeting for each, and write results.
(p
 | df.Read('load messages', df.io.TextFileSource('./names'))
 | df.Map('add greeting',
          lambda name, msg: '%s %s!' % (msg, name),
          'Hello')
 | df.Write('save', df.io.TextFileSink('./greetings')))
p.run()
```

### Hello world (with FlatMap)

A `FlatMap` is like a `Map` except its callable returns a (possibly
empty) iterable of elements for the output `PCollection`.

```python
import google.cloud.dataflow as df
p = df.Pipeline('DirectPipelineRunner')
# Read previous file, add a name to each greeting and write results.
(p
 | df.Read('load messages', df.io.TextFileSource('./names'))
 | df.FlatMap('add greetings',
              lambda name, msgs: ['%s %s!' % (m, name) for m in msgs],
              ['Hello', 'Hola'])
 | df.Write('save', df.io.TextFileSink('./greetings')))
p.run()
```

### Hello world (with FlatMap and yield)

The callable of a `FlatMap` can be a generator, that is,
a function using `yield`.

```python
import google.cloud.dataflow as df
p = df.Pipeline('DirectPipelineRunner')
# Add greetings using a FlatMap function using yield.
def add_greetings(name, messages):
  for m in messages:
    yield '%s %s!' % (m, name)

(p
 | df.Read('load names', df.io.TextFileSource('./names'))
 | df.FlatMap('greet', add_greetings, ['Hello', 'Hola'])
 | df.Write('save', df.io.TextFileSink('./greetings')))
p.run()
```

### Counting words

This example counts the words in a text and also shows how to read a
text file from [Google Cloud Storage](https://cloud.google.com/storage/).

```python
import re
import google.cloud.dataflow as df
p = df.Pipeline('DirectPipelineRunner')
(p
 | df.Read('read',
           df.io.TextFileSource(
           'gs://dataflow-samples/shakespeare/kinglear.txt'))
 | df.FlatMap('split', lambda x: re.findall(r'\w+', x))
 | df.combiners.Count.PerElement('count words')
 | df.Write('write', df.io.TextFileSink('./results')))
p.run()
```

### Counting words with GroupByKey

Here we use `GroupByKey` to count the words.
This is a somewhat forced example of `GroupByKey`; normally one would use
the transform `df.combiners.Count.PerElement`, as in the previous example.
The example also shows the use of a wild-card in specifying the text file
source.

```python
import re
import google.cloud.dataflow as df
p = df.Pipeline('DirectPipelineRunner')
class MyCountTransform(df.PTransform):
  def apply(self, pcoll):
    return (pcoll
    | df.Map('one word', lambda w: (w, 1))
    # GroupByKey accepts a PCollection of (w, 1) and
    # outputs a PCollection of (w, (1, 1, ...))
    | df.GroupByKey('group words')
    | df.Map('count words', lambda (word, counts): (word, len(counts))))

(p
 | df.Read('read', df.io.TextFileSource('./names*'))
 | df.FlatMap('split', lambda x: re.findall(r'\w+', x))
 | MyCountTransform()
 | df.Write('write', df.io.TextFileSink('./results')))
p.run()
```

### Type hints

In some cases, you can improve the efficiency of the data encoding by providing
type hints.  For example:

```python
import google.cloud.dataflow as df
from google.cloud.dataflow.typehints import typehints
p = df.Pipeline('DirectPipelineRunner')
(p
 | df.Read('A', df.io.TextFileSource('./names'))
 | df.Map('B1', lambda x: (x, 1)).with_output_types(typehints.KV[str, int])
 | df.GroupByKey('GBK')
 | df.Write('C', df.io.TextFileSink('./results')))
p.run()
```

### BigQuery

Here is a pipeline that reads input from a BigQuery table and writes the result
to a different table. This example calculates the number of tornadoes per month
from weather data. To run it you will need to provide an output table that
you can write to.

```python
import google.cloud.dataflow as df
# The output table needs to point to something in your project.
output_table = 'YOUR_PROJECT:DATASET.TABLE'
input_table = 'clouddataflow-readonly:samples.weather_stations'
p = df.Pipeline('DirectPipelineRunner')
(p
 | df.Read('read', df.io.BigQuerySource(input_table))
 | df.FlatMap(
     'months with tornadoes',
     lambda row: [(int(row['month']), 1)] if row['tornado'] else [])
 | df.CombinePerKey('monthly count', sum)
 | df.Map('format', lambda (k, v): {'month': k, 'tornado_count': v})
 | df.Write('write', df.io.BigQuerySink(
      output_table,
      schema='month:INTEGER, tornado_count:INTEGER',
      create_disposition=df.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=df.io.BigQueryDisposition.WRITE_TRUNCATE)))
p.run()
```

Here is a pipeline that achieves the same functionality, i.e., calculates the
number of tornadoes per month, but uses a query to filter out input instead
of using the whole table.

```python
import google.cloud.dataflow as df
# The output table needs to point to something in your project.
output_table = 'YOUR_PROJECT:DATASET.TABLE'
input_query = 'SELECT month, COUNT(month) AS tornado_count ' \
        'FROM [clouddataflow-readonly:samples.weather_stations] ' \
        'WHERE tornado=true GROUP BY month'
p = df.Pipeline('DirectPipelineRunner')
(p
| df.Read('read', df.io.BigQuerySource(query=input_query))
| df.Write('write', df.io.BigQuerySink(
    output_table,
    schema='month:INTEGER, tornado_count:INTEGER',
    create_disposition=df.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=df.io.BigQueryDisposition.WRITE_TRUNCATE)))
p.run()
```

### Combiner Examples

A common case for Dataflow combiners is to sum (or max or min) over the values
of each key. Such standard Python functions can be used directly as combiner
functions. In fact, any function "reducing" an iterable to a single value can be
used.

```python
import google.cloud.dataflow as df
p = df.Pipeline('DirectPipelineRunner')

SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20)]

(p
 | df.Create(SAMPLE_DATA)
 | df.CombinePerKey(sum)
 | df.Write(df.io.TextFileSink('./results')))
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
