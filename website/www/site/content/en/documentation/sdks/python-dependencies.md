---
type: languages
title: "Python SDK dependencies"
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

# Beam SDK for Python dependencies

This page provides the information about the Apache Beam Python SDK dependencies.

If your pipeline requires additional dependencies, see [Managing Python Pipeline Dependencies](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/).

Dependencies of the Apache Beam Python SDK are defined in the `setup.py` file in the Beam repository. To view them, take the following steps:

1. Open `setup.py`.

    ```
    https://github.com/apache/beam/blob/release-<VERSION_NUMBER>/sdks/python/setup.py
    ```

    <p class="paragraph-wrap">Replace `&lt;VERSION_NUMBER&gt;` with the major.minor.patch version of the SDK. For example, <a href="https://github.com/apache/beam/blob/release-{{< param release_latest >}}/sdks/python/setup.py" target="_blank" rel="noopener noreferrer">https://github.com/apache/beam/blob/release-{{< param release_latest >}}/sdks/python/setup.py</a> provides the dependencies for the {{< param release_latest >}} release.</p>


2. Review the core dependency list under `REQUIRED_PACKAGES`.

    **Note:** If you need [extra features](/get-started/quickstart-py#extra-requirements), such as `gcp` or `dataframe`, review the lists in `extras_require` for additional dependencies.

You can also retrieve the dependency list from the command line using the following process:

1.  Create a clean virtual environment on your local machine using a supported python version.

    ```
    $ python3 -m venv env && source ./env/bin/activate && pip install --upgrade pip setuptools wheel
    ```

2. [Install the Beam Python SDK](/get-started/quickstart-py/#download-and-install).

3. Retrieve the list of dependencies.

    ```
    $ pip install pipdeptree && pipdeptree -p apache-beam
    ```

If you have a `docker` installation, you can inspect the dependencies
preinstalled in Beam Python SDK [container
images](/documentation/runtime/environments/) by creating a container from an
image, for example: `docker run --rm -it --entrypoint=/bin/sh apache/beam_python3.10_sdk:2.55.0 -c "pip list"`.

You can also find the list of the dependencies installed in Beam containers in
`base_image_requirements.txt` files in the [Beam repository](https://github.com/apache/beam/blob/release-{{<param release_latest >}}/sdks/python/container) for a corresponding Beam release branch and Python minor version.
