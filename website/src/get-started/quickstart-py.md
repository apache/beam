---
layout: section
title: "Beam Quickstart for Python"
permalink: /get-started/quickstart-py/
section_menu: section-menu/get-started.html
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

# Apache Beam Python SDK Quickstart

This guide shows you how to set up your Python development environment, get the Apache Beam SDK for Python, and run an example pipeline.

If you're interested in contributing to the Apache Beam Python codebase, see the [Contribution Guide]({{ site.baseurl }}/contribute).

* TOC
{:toc}

The Python SDK supports Python 2.7, 3.5, 3.6, and 3.7. New Python SDK releases will stop supporting Python 2.7 in 2020 ([BEAM-8371](https://issues.apache.org/jira/browse/BEAM-8371)). For best results, use Beam with Python 3.

## Set up your environment

### Check your Python version

The Beam SDK requires Python 2 users to use Python 2.7 and Python 3 users to use Python 3.5 or higher. Check your version by running:

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

{:.shell-unix}
```
pip install --upgrade pip
```

{:.shell-PowerShell}
```
PS> python -m pip install --upgrade pip
```


### Install Python virtual environment

It is recommended that you install a [Python virtual environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)
for initial experiments. If you do not have `virtualenv` version 13.1.0 or
newer, run the following command to install it. This command might require
administrative privileges.

{:.shell-unix}
```
pip install --upgrade virtualenv
```

{:.shell-PowerShell}
```
PS> python -m pip install --upgrade virtualenv
```

If you do not want to use a Python virtual environment (not recommended), ensure
`setuptools` is installed on your machine. If you do not have `setuptools`
version 17.1 or newer, run the following command to install it.

{:.shell-unix}
```
pip install --upgrade setuptools
```

{:.shell-PowerShell}
```
PS> python -m pip install --upgrade setuptools
```

## Get Apache Beam

### Create and activate a virtual environment

A virtual environment is a directory tree containing its own Python distribution. To create a virtual environment, create a directory and run:

{:.shell-unix}
```
virtualenv /path/to/directory
```

{:.shell-PowerShell}
```
PS> virtualenv C:\path\to\directory
```

A virtual environment needs to be activated for each shell that is to use it.
Activating it sets some environment variables that point to the virtual
environment's directories.

To activate a virtual environment in Bash, run:

{:.shell-unix}
```
. /path/to/directory/bin/activate
```

{:.shell-PowerShell}
```
PS> C:\path\to\directory\Scripts\activate.ps1
```

That is, execute the `activate` script under the virtual environment directory you created.

For instructions using other shells, see the [virtualenv documentation](https://virtualenv.pypa.io/en/stable/userguide/#activate-script).

### Download and install

Install the latest Python SDK from PyPI:

{:.shell-unix}
```
pip install apache-beam
```

{:.shell-PowerShell}
```
PS> python -m pip install apache-beam
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
Currently, running wordcount.py on Flink requires a full download of the Beam source code.
See https://beam.apache.org/roadmap/portability/#python-on-flink for more information.
```

{:.runner-flink-cluster}
```
Currently, running wordcount.py on Flink requires a full download of the Beam source code.
See https://beam.apache.org/documentation/runners/flink/ for more information.
```

{:.runner-spark}
```
Currently, running wordcount.py on Spark requires a full download of the Beam source code.
See https://beam.apache.org/roadmap/portability/#python-on-spark for more information.
```

{:.runner-dataflow}
```
# As part of the initial setup, install Google Cloud Platform specific extra components. Make sure you
# complete the setup steps at {{ site.baseurl }}/documentation/runners/dataflow/#setup
pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
                                         --output gs://<your-gcs-bucket>/counts \
                                         --runner DataflowRunner \
                                         --project your-gcp-project \
                                         --temp_location gs://<your-gcs-bucket>/tmp/
```

{:.runner-nemo}
```
This runner is not yet available for the Python SDK.
```

After the pipeline completes, you can view the output files at your specified
output path. For example, if you specify `/dir1/counts` for the `--output`
parameter, the pipeline writes the files to `/dir1/` and names the files
sequentially in the format `counts-0000-of-0001`.

## Next Steps

* Learn more about the [Beam SDK for Python]({{ site.baseurl }}/documentation/sdks/python/)
  and look through the [Python SDK API reference](https://beam.apache.org/releases/pydoc).
* Walk through these WordCount examples in the [WordCount Example Walkthrough]({{ site.baseurl }}/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources]({{ site.baseurl }}/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts]({{ site.baseurl }}/documentation/resources/videos-and-podcasts).
* Join the Beam [users@]({{ site.baseurl }}/community/contact-us) mailing list.

Please don't hesitate to [reach out]({{ site.baseurl }}/community/contact-us) if you encounter any issues!
