---
title: "Beam Quickstart for Python"
aliases:
  - /get-started/quickstart/
  - /use/quickstart/
  - /getting-started/
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

# Apache Beam Python SDK quickstart

This quickstart shows you how to run an
[example pipeline](https://github.com/apache/beam-starter-python) written with
the [Apache Beam Python SDK](/documentation/sdks/python), using the
[Direct Runner](/documentation/runners/direct/). The Direct Runner executes
pipelines locally on your machine.

If you're interested in contributing to the Apache Beam Python codebase, see the
[Contribution Guide](/contribute).

On this page:

{{< toc >}}

## Set up your development environment

Apache Beam aims to work on released
[Python versions](https://devguide.python.org/versions/) that have not yet
reached end of life, but it may take a few releases until Apache Beam fully
supports the most recently released Python minor version.

The minimum required Python version is listed in the **Meta** section of the
[apache-beam](https://pypi.org/project/apache-beam/) project page under
**Requires**. The list of all supported Python versions is listed in the
**Classifiers** section at the bottom of the page, under **Programming
Language**.

Check your Python version by running:

{{< highlight >}}
python3 --version
{{< /highlight >}}

If you don't have a Python interpreter, you can download and install it from
the [Python downloads](https://devguide.python.org/versions/) page.

If you need to install a different version of Python in addition to the version
that you already have, you can find some recommendations in our
[Developer Wiki](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips#PythonTips-InstallingPythoninterpreters).

## Clone the GitHub repository

Clone or download the
[apache/beam-starter-python](https://github.com/apache/beam-starter-python)
GitHub repository and change into the `beam-starter-python` directory.

{{< highlight >}}
git clone https://github.com/apache/beam-starter-python.git
cd beam-starter-python
{{< /highlight >}}

## Create and activate a virtual environment

A virtual environment is a directory tree containing its own Python
distribution. We recommend using a virtual environment so that all dependencies
of your project are installed in an isolated and self-contained environment. To
set up a virtual environment, run the following commands:

{{< highlight >}}
# Create a new Python virtual environment.
python3 -m venv env

# Activate the virtual environment.
source env/bin/activate
{{< /highlight >}}

If these commands do not work on your platform, see the
[`venv`](https://docs.python.org/3/library/venv.html#how-venvs-work)
documentation.

## Install the project dependences

Run the following command to install the project's dependencies from the
`requirements.txt` file:

{{< highlight >}}
pip install -e .
{{< /highlight >}}

## Run the quickstart

Run the following command:

{{< highlight >}}
python main.py --input-text="Greetings"
{{< /highlight >}}

The output is similar to the following:

{{< highlight >}}
Hello
World!
Greetings
{{< /highlight >}}

The lines might appear in a different order.

Run the following command to deactivate the virtual environment:

{{< highlight >}}
deactivate
{{< /highlight >}}

## Explore the code

The main code file for this quickstart is **app.py**
([GitHub](https://github.com/apache/beam-starter-python/blob/main/my_app/app.py)).
The code performs the following steps:

1. Create a Beam pipeline.
3. Create an initial `PCollection`.
3. Apply a transform to the `PCollection`.
4. Run the pipeline, using the Direct Runner.

### Create a pipeline

The code first creates a `Pipeline` object. The `Pipeline` object builds up the
graph of transformations to be executed.

```python
with beam.Pipeline(options=beam_options) as pipeline:
```

The `beam_option` variable shown here is a `PipelineOptions` object, which
is used to set options for the pipeline. For more information, see
[Configuring pipeline options](/documentation/programming-guide/#configuring-pipeline-options).

### Create an initial PCollection

The `PCollection` abstraction represents a potentially distributed,
multi-element data set. A Beam pipeline needs a source of data to populate an
initial `PCollection`. The source can be bounded (with a known, fixed size) or
unbounded (with unlimited size).

This example uses the
[`Create`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Create)
method to create a `PCollection` from an in-memory array of strings. The
resulting `PCollection` contains the strings "Hello", "World!", and a
user-provided input string.

```python
pipeline
| "Create elements" >> beam.Create(["Hello", "World!", input_text])
```

Note: The pipe operator `|` is used to
[chain](/documentation/programming-guide/#applying-transforms) transforms.

### Apply a transform to the PCollection

Transforms can change, filter, group, analyze, or otherwise process the
elements in a `PCollection`. This example uses the
[`Map`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Map)
transform, which maps the elements of a collection into a new collection:

```python
| "Print elements" >> beam.Map(print)
```

### Run the pipeline

To run the pipeline, you can call the
[`Pipeline.run`](https://beam.apache.org/releases/pydoc/current/apache_beam.pipeline.html#apache_beam.pipeline.Pipeline.run)
method:

```python
pipeline.run.wait_until_finish()
```

However, by enclosing the `Pipeline` object inside a `with` statement, the
`run` method is automatically invoked.


```python
with beam.Pipeline(options=beam_options) as pipeline:
    # ...
    # run() is called automatically
```

A Beam [runner](/documentation/basics/#runner) runs a Beam pipeline on a
specific platform. If you don't specify a runner, the Direct Runner is the
default. The Direct Runner runs the pipeline locally on your machine. It is
meant for testing and development, rather than being optimized for efficiency.
For more information, see
[Using the Direct Runner](/documentation/runners/direct/).

For production workloads, you typically use a distributed runner that runs the
pipeline on a big data processing system such as Apache Flink, Apache Spark, or
Google Cloud Dataflow. These systems support massively parallel processing.

## Next Steps

* Learn more about the [Beam SDK for Python](/documentation/sdks/python/)
  and look through the
  [Python SDK API reference](https://beam.apache.org/releases/pydoc/current).
* Take a self-paced tour through our
  [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite
  [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any
issues!

