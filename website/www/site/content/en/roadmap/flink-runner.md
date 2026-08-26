---
title: "Flink Runner Roadmap"
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

# Apache Flink Runner Roadmap

_Last updated on Aug 2026._

## Flink versions

Beam aims to support latest Flink minor versions. See [Flink Runner Support Table](/documentation/runners/flink#flink-versions-supported-by-beam-releases)
for current coverage.

### Flink 2.x Support

One of the major changes in Flink 2 is the removal of Flink DataSet API. As a result Beam Flink 2 runners switched to use DataStream API for batch mode. One area of interest is to improve the batch support of DataStream API.

## Available resources

Please check GitHub Issue tracker for recent developments on this topic:

 - Issues: [runner-flink](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Aflink)

- [Runner documentation](/documentation/runners/flink)
