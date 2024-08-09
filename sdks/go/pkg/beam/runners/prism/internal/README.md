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

# Prism internal packages

Go has a mechanism for ["internal" packages](https://go.dev/doc/go1.4#internalpackages)
to prevent use of implementation details outside of their intended use.

This mechanism is used thoroughly for Prism to ensure we can make changes to the
runner's internals without worrying about the exposed surface changes breaking
non-compliant users.

# Structure

Here's a loose description of the current structure of the runner. Leaf packages should
not depend on other parts of the runner. Runner packages can and do depend on other
parts of the SDK, such as for Coder handling.

`config` contains configuration parsing and handling. Leaf package.
Handler configurations are registered by dependant packages.

`urns` contains beam URN strings pulled from the protos. Leaf package.

`engine` contains the core manager for handling elements, watermarks, and windowing strategies.
Determines bundle readiness, and stages to execute. Leaf package.

`jobservices` contains GRPC service handlers for job management and submission.
Should only depend on the `config` and `urns` packages.

`worker` contains interactions with FnAPI services to communicate with worker SDKs. Leaf package
except for dependency on `engine.TentativeData` which will likely be removed at some point.

`internal` AKA the package in this directory root. Contains fhe job execution
flow. Jobs are sent to it from `jobservices`, and those jobs are then executed by coordinating
with the `engine` and `worker` packages, and handlers.
Most configurable behavior is determined here.

`web` contains a web server to visualize aspects of the runner, and should never be
depended on by the other packages for the runner.
It will primarily abstract access to pipeline data through a JobManagement API client.
If no other option exists, it may use direct access to exported data from the other packages.

# Testing

The sub packages should have reasonable Unit Test coverage in their own directories, but
most features will be exercised via executing pipelines in this package.

For the time being test DoFns should be added to standard build in order to validate execution
coverage, in particular for Combine and Splittable DoFns.

Eventually these behaviors should be covered by using Prism in the main SDK tests.
