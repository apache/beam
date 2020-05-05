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

* We hope to add a Splittable DoFn implementation to Go SDK to support developing new sources. In the
meantime bounded sources can be developed in the form of ParDos. See
[Authoring I/O Transforms](https://beam.apache.org/documentation/io/authoring-overview/) for more details.

* Go SDK plans to utilize currently available Java and Python connectors
through cross-language transforms feature.
