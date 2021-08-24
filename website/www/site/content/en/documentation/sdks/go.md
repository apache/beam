---
type: languages
title: "Beam Go SDK"
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
# Apache Beam Go SDK

The Go SDK for Apache Beam provides a simple, powerful API for building both batch and streaming parallel data processing pipelines.
It is based on the following [design](https://s.apache.org/beam-go-sdk-design-rfc).

## Get Started with the Go SDK

Get started with the [Beam Go SDK quickstart](/get-started/quickstart-go) to set up your development environment and run an example pipeline. Then, read through the [Beam programming guide](/documentation/programming-guide) to learn the basic concepts that apply to all SDKs in Beam.

See the [godoc](https://pkg.go.dev/github.com/apache/beam/sdks/go/pkg/beam) for more detailed information.

## Status

Version 2.32.0 is the last experimental release of the Go SDK. The Go SDK supports most Batch oriented features, and cross language transforms.
It's possible to write many kinds of transforms, but specific built in transforms may still be missing.

Requests for specific transforms may be filed to the [`sdk-go` component in JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20status%20in%20(Open%2C%20%22In%20Progress%22%2C%20Idea%2C%20%22Under%20Discussion%22%2C%20%22In%20Implementation%22%2C%20%22Triage%20Needed%22)%20AND%20component%20%3D%20sdk-go%20ORDER%20BY%20updated%20DESC).
Contributions are welcome.
