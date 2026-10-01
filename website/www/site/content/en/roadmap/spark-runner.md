---
title: "Spark Runner Roadmap"
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

# Apache Spark Runner Roadmap

_Last updated on Aug 2026._

## Spark 4

Support for Spark 4 in Beam's Spark runner is ongoing.

As of Beam 2.74.0, Spark 4 is supported by Classic Spark runner in batch mode.

As of Spark 4, DStream API is deprecated and community interest is around Structured Streaming. We are working on improving the Structured Streaming support in Beam's Spark runner, this includes
- Complete streaming support for Spark 4 based on Structured Streaming
- Build Spark 4 portable runner based on Structured Streaming

For info on the various tasks please refer to the GitHub Issue.

- Issues: [runner-spark](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Aspark)
