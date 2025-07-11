---
title: "Apache Beam Roadmap Highlights"
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

# Apache Beam Roadmap

Apache Beam is not governed or steered by any one commercial entity, but by its
Project Management Committee (PMC), so we do not have a roadmap in the sense of
a plan with a specific timeline.
Instead, we share our vision for the future and major initiatives receiving
and attention and contribution that users can look forward to.

The major components of Beam each have their own roadmap which you can find
via the menu.
Below are some highlights for the project as a whole.

## Portability Framework

Portability is the primary Beam vision: running pipelines authored with _any SDK_
on _any runner_. This is a cross-cutting effort across Java, Python, and Go,
and every Beam runner. Portability is currently supported on the
[DataFlow](/documentation/runners/dataflow), [Flink](/documentation/runners/flink/), [Jet](/documentation/runners/jet), [Nemo](/documentation/runners/nemo), [Prism](/documentation/runners/prism/), [Samza](/documentation/runners/samza), [Spark](/documentation/runners/spark/), and [Twister2](/documentation/runners/Twister2)
runners.

See the details on the [Portability Roadmap](/roadmap/portability/)

## Cross-language transforms

As a benefit of the portability effort, we are able to utilize Beam transforms across SDKs.
Examples include using Java connectors and Beam SQL from Python or Go pipelines
or Beam TFX transforms from Java and Go.
For details see [Roadmap for multi-SDK efforts](/roadmap/connectors-multi-sdk/).

## Go SDK

The Go SDK is not actively being developed beyond bugfixes due to lack of contributors. If you'd like to help move this forward again, see the [Go SDK's Roadmap](/roadmap/go-sdk)

## Python 3 support

As of Apache Beam 2.61.0, Python 3.8 support has been removed. We support python version from 3.9 uptil Python 3.12. Supporting Python 3.13 is in our roadmap.

See details on
the [Python SDK's Roadmap](/roadmap/python-sdk/#python-3-support).

## Java 17 support

Java 17 is already supported and Java's next LTS (Long Term Support)
version (21) is already on roadmap. See details on
the [Java SDK's Roadmap](/roadmap/java-sdk).

## SQL

Beam's SQL module is rapidly maturing to allow users to author batch and
streaming pipelines using only SQL, but also to allow Beam Java developers
to use SQL in components of their pipeline for added efficiency. See the
[Beam SQL Roadmap](/roadmap/sql/)

## Portable schemas

Schemas allow SDKs and runners to understand
the structure of user data and unlock relational optimization possibilities.
Portable schemas enable compatibility between rows in Python and Java.
A particularly interesting use case is the combination of SQL (implemented in Java)
with the Python SDK via Beam's cross-language support.
Learn more about portable schemas from this [presentation](https://s.apache.org/portable-schemas-seattle).

## Euphoria

Euphoria is Beam's newest API, offering a high-level, fluent style for
Beam Java developers. See the [Euphoria API Roadmap](/roadmap/euphoria).

