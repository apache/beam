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

# Overview
Apache Beam provides a portable API layer for building sophisticated data-parallel processing pipelines that may be executed across a diversity of execution engines, or runners. The core concepts of this layer are based upon the Beam Model (formerly referred to as the Dataflow Model), and implemented to varying degrees in each Beam runner.
Apache Beam maintains a capability matrix to track which Beam features are supported by which set of language SDKs + Runners.

This module consists of the scripts to automatically update the capability matrix with each project release so that its uptodate up to date with minimum supervision or ownership.
The workflow works as follows:

- The script will run periodically, and using the latest runs from relevant test suites. The script outputs a capability matrix file in JSON format.
- The capability matrix file will be uploaded to a public folder in GCS
- The Beam website will fetch the capability matrix file every time a user loads the Capability Matrix pagefile, and build the matrix

###Run the project
This module can be run using the below command. It accepts a single argument which is the output JSON filename. If not passes, the output will be written to the file capability.json

`./gradlew beam-validate-runner:runner -Pargs="filename"`

####Run Configurations
The project includes a [configuration file](src/main/resources/configuration.yaml) which includes the different configurations to generate the capablities.
Inoreder to add a new runner, the runner name and the Jenkins job name needs to be added to the [configuration file](src/main/resources/configuration.yaml) in the respective mode(batch/stream).
