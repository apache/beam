---
title: "Connectors - Go SDK"
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

Roadmap for connectors developed using Go SDK.

* Go SDK plans to utilize currently available Java and Python connectors
through cross-language transforms feature.
  * KafkaIO via Java - DONE
  * BigQuery via Java - In Progress
  * Beam SQL via Java
* The Go SDK supports SplittableDoFns for bounded pipelines, so scalable bounded pipelines are possible.
    * The textio package supports [ReadSdf](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio#ReadSdf) and [ReadAllSdf](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio#ReadAllSdf) for efficient batch text reads.
    * A general FileIO will be produced to simplify adding new file based connectors.
