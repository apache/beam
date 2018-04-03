---
layout: section
title: "Beam Quickstart for Python"
permalink: /get-started/quickstart-py/
section_menu: section-menu/get-started.html
---

# Apache Beam Python SDK Quickstart

This guide shows you how to set up your Python development environment, get the Apache Beam SDK for Python, and run an example pipeline.

* TOC
{:toc}

## Set up your environment

### Check your Python version

The Beam SDK for Python requires Python version 2.7.x. Check that you have version 2.7.x by running:

```
python --version
```

### Install pip

Install [pip](https://pip.pypa.io/en/stable/installing/), Python's package manager. Check that you have version 7.0.0 or newer by running:

```
pip --version
```

If you do not have `pip` version 7.0.0 or newer, run the following command to
install it. This command might require administrative privileges.

```
pip install --upgrade pip
```


### Install Python virtual environment

It is recommended that you install a [Python virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
for initial experiments. If you do not have `virtualenv` version 13.1.0 or
newer, run the following command to install it. This command might require
administrative privileges.

```
pip install --upgrade virtualenv
```

If you do not want to use a Python virtual environment (not recommended), ensure
`setuptools` is installed on your machine. If you do not have `setuptools`
version 17.1 or newer, run the following command to install it.

```
pip install --upgrade setuptools
```

## Get Apache Beam

### Create and activate a virtual environment

A virtual environment is a directory tree containing its own Python distribution. To create a virtual environment, create a directory and run:

```
virtualenv /path/to/directory
```

A virtual environment needs to be activated for each shell that is to use it.
Activating it sets some environment variables that point to the virtual
environment's directories.

To activate a virtual environment in Bash, run:

```
. /path/to/directory/bin/activate
```

That is, source the script `bin/activate` under the virtual environment directory you created.

For instructions using other shells, see the [virtualenv documentation](https://virtualenv.pypa.io/en/stable/userguide/#activate-script).

### Download and install

Install the latest Python SDK from PyPI:

```
pip install apache-beam
```

#### Extra requirements

The above installation will not install all the extra dependencies for using features like the Google Cloud Dataflow runner. Information on what extra packages are required for different features are highlighted below. It is possible to install multitple extra requirements using something like `pip install apache-beam[feature1,feature2]`.

- **Google Cloud Platform**
  - Installation Command: `pip install apache-beam[gcp]`
  - Required for:
    - Google Cloud Dataflow Runner
    - GCS IO
    - Datastore IO
    - BigQuery IO
- **Tests**
  - Installation Command: `pip install apache-beam[test]`
  - Required for developing on beam and running unittests
- **Docs**
  - Installation Command: `pip install apache-beam[docs]`
  - Generating API documentation using Sphinx

## Execute a pipeline

The Apache Beam [examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples) directory has many examples. All examples can be run locally by passing the required arguments described in the example script.

For example, run `wordcount.py` with the following command:

{:.runner-direct}
```
python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts
```

{:.runner-apex}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-local}
```
This runner is not yet available for the Python SDK.
```

{:.runner-flink-cluster}
```
This runner is not yet available for the Python SDK.
```

{:.runner-spark}
```
This runner is not yet available for the Python SDK.
```

{:.runner-dataflow}
```
# As part of the initial setup, install Google Cloud Platform specific extra components. Make sure you
# complete the setup steps at https://beam.apache.org/documentation/runners/dataflow/#setup
pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
                                         --output gs://<your-gcs-bucket>/counts \
                                         --runner DataflowRunner \
                                         --project your-gcp-project \
                                         --temp_location gs://<your-gcs-bucket>/tmp/
```

After the pipeline completes, you can view the output files at your specified
output path. For example, if you specify `/dir1/counts` for the `--output`
parameter, the pipeline writes the files to `/dir1/` and names the files
sequentially in the format `counts-0000-of-0001`.

## Next Steps

* Learn more about the [Beam SDK for Python]({{ site.baseurl }}/documentation/sdks/python/)
  and look through the [Python SDK API reference]({{ site.baseurl }}/documentation/sdks/pydoc).
* Walk through these WordCount examples in the [WordCount Example Walkthrough]({{ site.baseurl }}/get-started/wordcount-example).
* Dive in to some of our favorite [articles and presentations]({{ site.baseurl }}/documentation/resources).
* Join the Beam [users@]({{ site.baseurl }}/get-started/support#mailing-lists) mailing list.

Please don't hesitate to [reach out]({{ site.baseurl }}/get-started/support) if you encounter any issues!
