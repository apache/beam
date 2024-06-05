---
title: "WordCount Quickstart for Python"
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

# WordCount quickstart for Python

This guide shows you how to set up your Python development environment, get the Apache Beam SDK for Python, and run an example pipeline.

If you're interested in contributing to the Apache Beam Python codebase, see the [Contribution Guide](/contribute).

{{< toc >}}

The Python SDK supports Python 3.8, 3.9, 3.10 and 3.11. Beam 2.48.0 was the last release with support for Python 3.7.

## Set up your environment

For details, see
[Set up your development environment](/get-started/quickstart/python#set-up-your-development-environment).

## Get Apache Beam

### Create and activate a virtual environment

A virtual environment is a directory tree containing its own Python distribution. To create a virtual environment, run:

{{< shell unix >}}
python -m venv /path/to/directory
{{< /shell >}}

{{< shell powerShell >}}
PS> python -m venv C:\path\to\directory
{{< /shell >}}

A virtual environment needs to be activated for each shell that is to use it.
Activating it sets some environment variables that point to the virtual
environment's directories.

To activate a virtual environment in Bash, run:

{{< shell unix >}}
. /path/to/directory/bin/activate
{{< /shell >}}

{{< shell powerShell >}}
PS> C:\path\to\directory\Scripts\activate.ps1
{{< /shell >}}

That is, execute the `activate` script under the virtual environment directory you created.

For instructions using other shells, see the [venv documentation](https://docs.python.org/3/library/venv.html).

### Download and install

Install the latest Python SDK from PyPI:

{{< shell unix >}}
pip install apache-beam
{{< /shell >}}

{{< shell powerShell >}}
PS> python -m pip install apache-beam
{{< /shell >}}

#### Extra requirements

The above installation will not install all the extra dependencies for using features like the Google Cloud Dataflow runner. Information on what extra packages are required for different features are highlighted below. It is possible to install multiple extra requirements using something like `pip install 'apache-beam[feature1,feature2]'`.

- **Google Cloud Platform**
  - Installation Command: `pip install 'apache-beam[gcp]'`
  - Required for:
    - Google Cloud Dataflow Runner
    - GCS IO
    - Datastore IO
    - BigQuery IO
- **Amazon Web Services**
  - Installation Command: `pip install 'apache-beam[aws]'`
  - Required for I/O connectors interfacing with AWS
- **Microsoft Azure**
  - Installation Command: `pip install 'apache-beam[azure]'`
  - Required for I/O connectors interfacing with Microsoft Azure
- **Beam YAML API**
  - Installation Command: `pip install 'apache-beam[yaml]'`
  - Required for using [Beam YAML API](/documentation/sdks/yaml/)
- **Beam YAML Dataframe API**
  - Installation Command: `pip install 'apache-beam[dataframe]'`
  - Required for using [Beam Dataframe API](/documentation/dsls/dataframes/overview/)
- **Tests**
  - Installation Command: `pip install 'apache-beam[test]'`
  - Required for developing Beam and running unit tests
- **Docs**
  - Installation Command: `pip install 'apache-beam[docs]'`
  - Required for generating API documentation using Sphinx

## Execute a pipeline

The Apache Beam [examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples) directory has many examples. All examples can be run locally by passing the required arguments described in the example script.

For example, run `wordcount.py` with the following command:

{{< runner direct >}}
python -m apache_beam.examples.wordcount --input /path/to/inputfile --output /path/to/write/counts
{{< /runner >}}

{{< runner flink >}}
python -m apache_beam.examples.wordcount --input /path/to/inputfile \
                                         --output /path/to/write/counts \
                                         --runner FlinkRunner
{{< /runner >}}

{{< runner spark >}}
python -m apache_beam.examples.wordcount --input /path/to/inputfile \
                                         --output /path/to/write/counts \
                                         --runner SparkRunner
{{< /runner >}}

{{< runner dataflow >}}
# As part of the initial setup, install Google Cloud Platform specific extra components. Make sure you
# complete the setup steps at /documentation/runners/dataflow/#setup
pip install apache-beam[gcp]
python -m apache_beam.examples.wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
                                         --output gs://<your-gcs-bucket>/counts \
                                         --runner DataflowRunner \
                                         --project your-gcp-project \
                                         --region your-gcp-region \
                                         --temp_location gs://<your-gcs-bucket>/tmp/
{{< /runner >}}

{{< runner nemo >}}
This runner is not yet available for the Python SDK.
{{< /runner >}}

After the pipeline completes, you can view the output files at your specified
output path. For example, if you specify `/dir1/counts` for the `--output`
parameter, the pipeline writes the files to `/dir1/` and names the files
sequentially in the format `counts-0000-of-0001`.

## Next Steps

* Learn more about the [Beam SDK for Python](/documentation/sdks/python/)
  and look through the [Python SDK API reference](https://beam.apache.org/releases/pydoc).
* Get [An Interactive Overview of Beam](/get-started/an-interactive-overview-of-beam)
* Walk through these WordCount examples in the [WordCount Example Walkthrough](/get-started/wordcount-example).
* Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite [Videos and Podcasts](/get-started/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
