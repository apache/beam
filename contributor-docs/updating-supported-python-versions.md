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

# Adding/Removing Python Versions in Apache Beam

Python releases are now on an annual cadence, with new versions being released (and an old version reaching end-of-life) in October of a given year. This means that at any given time, Beam could be supporting up to five different versions of Python. Removing EOL versions is a higher priority than adding new versions, as EOL Python versions may not get vulnerability fixes when dependencies fix them.

## Adding a Python Version

1. Upgrade Beam direct dependencies to versions that support the new Python versions. Complex libraries, like pyarrow or numpy need to provide wheels for the new Python version. Infrastructure libraries, such as Beam build dependencies, cibuildwheel, and other libraries with a hardcoded version, may have to be upgraded as well.
    * Some dependency versions may not support both the minimum and maximum Python version for Beam and will require version-specific dependencies.

1. Add a Beam Python container for the new Python version.
    * https://github.com/apache/beam/tree/master/sdks/python/container

1. Add a new Python version to different test suites:
    * [Tox test suites](https://github.com/apache/beam/blob/master/sdks/python/tox.ini)
    * Gradle tasks such as pre-commits, post-commits etc.
    * Runner-specific versioning checks
    * Fix any tests that fail on the new Python version.
        * Typically, a new Python version requires updating Beam Type Inference code. See https://github.com/apache/beam/issues/31047

1. Add the GitHub actions workflows for the new Python version.
    * Example: https://github.com/apache/beam/blob/master/.github/workflows/python_tests.yml
    * The minimum and maximum Python versions are defined in a number of workflows and the [test-properties.json](https://github.com/apache/beam/blob/ce1b1dcbc596d1e7c914ee0f7b0d48f2d2bf87e1/.github/actions/setup-default-test-properties/test-properties.json) file, there will be potentially hundreds of changes for this step.

1. Add support for building wheels for the new Python version.
    * https://github.com/apache/beam/blob/master/.github/workflows/build_wheels.yml

1. Update the upper limit in [__init__.py](https://github.com/apache/beam/blob/0ef5d3a185c1420da118208353ceb0b40b3a27c9/sdks/python/apache_beam/__init__.py#L78) with the next major Python version.

1. Add the new Python version in release validation scripts: https://github.com/apache/beam/pull/31415

* If there is a new feature update or there is a regression when adding a new Python version, please file an [issue](https://github.com/apache/beam/issues).
    * **All the unit tests and Integration tests must pass before merging the new version.**
    *  If you are a non-committer, please ask the committers to run a seed job on your PR to test all the new changes.

For an example, see PRs associated with https://github.com/apache/beam/issues/29149, and commits on https://github.com/apache/beam/pull/30828 which add Python 3.12 support.

## Removing a Python Version

1. Bump the Python version in [setup.py](https://github.com/apache/beam/blob/0ef5d3a185c1420da118208353ceb0b40b3a27c9/sdks/python/setup.py#L152) and update the Python version warning in [__init__.py](https://github.com/apache/beam/blob/0ef5d3a185c1420da118208353ceb0b40b3a27c9/sdks/python/apache_beam/__init__.py#L78). 

1. Remove test suites for the unsupported Python version:
    * Migrate GitHub actions workflows from the deprecated Python version to the next one
        * Example PR: https://github.com/apache/beam/pull/32429
        * Make these changes on a branch in the main Beam repository if possible so you can execute the new workflows directly for testing.
        * Some workflows only run on the minimum supported Python version (like the linting and coverage precommits.) These may utilize libraries that need updates to run on the next Python version.
    * Remove the unsupported Python version from the following files/directories:
        * sdks/python/test-suites/gradle.properties
        * apache_beam/testing/tox
            Move any workflows that exist only for the minimum Python version from tox/py3X to the next minimum Python version's folder
        * apache_beam/testing/dataflow
        * apache_beam/testing/direct
        * apache_beam/testing/portable
    * Remove the unsupported Python version gradle tasks from
        * build.gradle.kts
        * settings.gradle.kts
        * buildSrc/src/main/groovy/org/apache/beam/gradle/BeamModulePlugin.groovy
    * Remove the support for building wheels and source distributions for the unsupported Python version from [.github/workflows/build_wheels.yml](https://github.com/apache/beam/blob/ce1b1dcbc596d1e7c914ee0f7b0d48f2d2bf87e1/.github/workflows/build_wheels.yml)
    * Remove the unsupported Python version from [sdks/python/tox.ini](https://github.com/apache/beam/blob/master/sdks/python/tox.ini)


1. Delete the unsupported Python version containers from [sdks/python/container](https://github.com/apache/beam/tree/master/sdks/python/container)

1. Clean up any code that applies to the removed Python version.
    * This will usually be version-specific dependencies in setup.py or branches in the typehinting module.