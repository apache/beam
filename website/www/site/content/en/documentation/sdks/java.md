---
type: languages
title: "Beam Java SDK"
aliases: /learn/sdks/java/
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
# Apache Beam Java SDK

The Java SDK for Apache Beam provides a simple, powerful API for building both batch and streaming parallel data processing pipelines in Java.


## Get Started with the Java SDK

Get started with the [Beam Programming Model](/documentation/programming-guide/) to learn the basic concepts that apply to all SDKs in Beam.

See the [Java API Reference](https://beam.apache.org/releases/javadoc/) for more information on individual APIs.


## Supported Features

The Java SDK supports all features currently supported by the Beam model.


## Pipeline I/O
See the [Beam-provided I/O Transforms](/documentation/io/built-in/) page for a list of the currently available I/O transforms.


## Extensions

The Java SDK has the following extensions:

- [join-library](/documentation/sdks/java-extensions/#join-library) provides inner join, outer left join, and outer right join functions.
- [sorter](/documentation/sdks/java-extensions/#sorter) is an efficient and scalable sorter for large iterables.
- [Nexmark](/documentation/sdks/java/testing/nexmark) is a benchmark suite that runs in batch and streaming modes.
- [euphoria](/documentation/sdks/java/euphoria) is easy to use Java 8 DSL for BEAM.

In addition several [3rd party Java libraries](/documentation/sdks/java-thirdparty/) exist.
