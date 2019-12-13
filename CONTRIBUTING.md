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

# How to Contribute

The Apache Beam community welcomes contributions from anyone!
Please see our [contribution guide](https://beam.apache.org/contribute/contribution-guide/)
for details, such as:

* Sharing your intent with the community
* Development setup and testing your changes
* Submitting a pull request and finding a reviewer

To build and install the whole project from the source distribution,
you need additional tools installed in your system including:

* Go 1.12 or later
* JDK 8 or later
* Python 3.5 or later
* pip
* setuptools
* virtualenv
* tox
* Docker

To install these in a Debian-based distribution:

```
sudo apt-get install \
    openjdk-8-jdk \
    python-setuptools \
    python-pip \
    virtualenv \
    tox \
    docker-ce
```

You also need to [install Go](https://golang.org/doc/install]).

Then use the standard `./gradlew build` command.

