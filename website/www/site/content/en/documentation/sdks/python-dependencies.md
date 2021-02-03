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

The Beam SDKs depend on common third-party components which then
import additional dependencies. Version collisions can result in unexpected
behavior in the service. If you are using any of these packages in your code, be
aware that some libraries are not forward-compatible and you may need to pin to
the listed versions that will be in scope during execution.

Dependencies for your Beam SDK version are listed in `setup.py` in the Beam repository. To view them, perform the following steps:

1. Open `setup.py`.

    ```
    https://raw.githubusercontent.com/apache/beam/v<VERSION_NUMBER>/sdks/python/setup.py
    ```

    Replace `<VERSION_NUMBER>` with the major.minor.patch version of the SDK. For example, <a href="ttps://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/sdks/python/setup.py" target="_blank">https://raw.githubusercontent.com/apache/beam/v{{< param release_latest >}}/sdks/python/setup.py</a> will provide the dependencies for the {{< param release_latest >}} release.


2. Review the core dependency list under `REQUIRED_PACKAGES`.

    **Note:** If you require [extra features](/get-started/quickstart-py#extra-requirements) such as `gcp` or `test`, you should review the lists under `REQUIRED_TEST_PACKAGES`, `GCP_REQUIREMENTS`, or `INTERACTIVE_BEAM` for additional dependencies.

You can also retrieve the dependency list from the command line using the following process:

1.  Create a clean virtual environment on your local machine.

    Python 3:

    ```
    $ python3 -m venv env && source env/bin/activate
    ```

    Python 2:

    ```
    $ pip install virtualenv && virtualenv env && source env/bin/activate
    ```

2. [Install the Beam Python SDK](/get-started/quickstart-py/#download-and-install).

3. Retrieve the list of dependencies.

    ```
    $ pip install pipdeptree && pipdeptree -p apache-beam
    ```
