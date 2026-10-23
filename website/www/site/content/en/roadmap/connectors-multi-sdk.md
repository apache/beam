---
title: "Multi-SDK Connector Efforts"
aliases:
  - /roadmap/connectors-java-sdk/
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

Roadmap for connectors and cross-cutting efforts that benefit multiple SDKs.

_Last updated on Aug 2026._

# Extended Metadata and SDK level CDC (Change Data Capture) support

Beam is introducing **Extended Metadata** to make element values extensible across SDKs. This provides native support for Change Data Capture (CDC) operations (`INSERT`, `UPDATE`, `DELETE`) consumed by lakehouse and database connectors, SDK-level pipeline drain, and fluent `OutputBuilder` APIs.

Learn more from the [Beam Element Extended Metadata Design Doc](https://s.apache.org/beam-element-extended-metadata).

# OpenTelemetry Support

Beam is adding native [OpenTelemetry](https://opentelemetry.io/) support across SDKs, runners, and connectors ([#33176](https://github.com/apache/beam/issues/33176)) for distributed tracing. This leverages Extended Metadata to propagate W3C Trace Context (`traceparent` and `tracestate`) across transform stages and worker boundaries, integrates trace header propagation into streaming I/Os (such as Kafka, Pub/Sub, and Spanner change streams), and enables runner-level trace and log correlation.

# IO Connector Ecosystem

Beam is actively expanding the Beam I/O connector portfolio with a focus on modern data lakehouse formats, native Change Data Capture (CDC), and unified access patterns:

* **First-Class Lakehouse Connectors**:
  * **[Apache Iceberg](/documentation/io/built-in/iceberg/)**: Full support for batch and streaming reads, streaming appends, dynamic destinations, and changelog CDC reading. Available natively in Java, via cross-language transforms in Python, in [Beam YAML](/documentation/sdks/yaml/), and via the [Managed I/O](/documentation/io/managed-io/) API.
  * **Delta Lake**: Native read capabilities ([#38551](https://github.com/apache/beam/issues/38551)) and batch changelog (CDC) reading ([#39492](https://github.com/apache/beam/issues/39492)), exposed through core transforms and the [Managed I/O](/documentation/io/managed-io/) API.
* **Managed I/O Expansion**: Exposing more storage systems through the [Managed I/O API](/documentation/io/managed-io/), which standardizes connector configurations using Beam Schemas, provides frictionless cross-language access (Java, Python, YAML), and enables runner-level optimizations.
* **Native CDC Integration**: Integrating database change streams (such as Spanner change streams and Debezium-based sources) directly into lakehouse sinks using Beam's native CDC `ValueKind` metadata.

See the full list of available connectors in the [Beam Connectors Overview](/documentation/io/connectors/).
