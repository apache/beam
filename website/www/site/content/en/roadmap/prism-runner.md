---
title: "Prism Runner Roadmap"
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

# Apache Beam Prism Runner Roadmap

The goal for the Prism runner is to provide a good default onboarding experience for Apache Beam.

* Prism should be able to execute any Beam pipeline that can execute on a local machine.
* Prism should be fast to start and execute pipelines.
* Prism should be able to assist with the local testing and debugging of pipelines.
* Prism may develop into a robust, production ready runner for pipelines that can execute locally.

The detailed roadmap lives in an [umbrella tracking issue in Github](https://github.com/apache/beam/issues/29650).

Here are available resources:

 - [Runner documentation](/documentation/runners/prism)
 - Issues: [prism](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Aprism)
 - CLI [Code](https://github.com/apache/beam/tree/master/sdks/go/cmd/prism), CLI Binaries are available as assets on [Github Releases](https://github.com/apache/beam/releases/tag/v{{< param release_latest >}}).
 - Core [Code](https://github.com/apache/beam/tree/master/sdks/go/pkg/beam/runners/prism)
 - [Prism Internals Deep Dive](https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/runners/prism/internal/README.md)
