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

# Apache Beam website

These are the main sources of the website for Apache Beam, hosted at
https://beam.apache.org/.

## About

The Beam website is built using [Hugo](https://gohugo.io/). Additionally,
for additional formatting capabilities, this website uses
[Twitter Bootstrap](https://getbootstrap.com/).

Documentation generated from source code, such as Javadoc and Pydoc, is stored
separately on the [beam-site
repository](https://github.com/apache/beam-site/tree/release-docs).

## Getting started

Website development requires Docker installed if you wish to preview changes and
run website tests.

The Docsy theme required for the site to work properly is included as a git submodule. This means that after you already cloned the repository, you need to update submodules at `<ROOT_DIRECTORY>`.

`$ git submodule update --init --recursive`

The following command is used to build and serve the website locally. Note: you should run the command at `<ROOT_DIRECTORY>`.

`$ ./gradlew :website:serveWebsite`

Any changes made locally will trigger a rebuild of the website.

Websites tests may be run using this command:

`$ ./gradlew :website:testWebsite`

For a more detailed description, please refer to the [contribution guide](CONTRIBUTE.md).

## Deployment

After a PR is merged, a background Jenkins job will automatically generate and
push [website
content](https://github.com/apache/beam/tree/asf-site/website/generated-content)
to the asf-site branch. This content is later picked up and pushed to
https://beam.apache.org/.

## Contribution guide

If you'd like to contribute to the Apache Beam website, read our [contribution guide](CONTRIBUTE.md) where you can find detailed instructions on how to work with the website.