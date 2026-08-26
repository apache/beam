---
title: "Connectors - Java SDK"
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

Roadmap for connectors developed using Java SDK.

_Last updated on Aug 2026._

# Extended Metadata and SDK level CDC (Change Data Capture) support

Beam is introducing **Extended Metadata** to make element values extensible. The support is first being added to Java SDK, and then will be propagated to other SDKs. This provides native, cross-SDK support for:

* **Change Data Capture (CDC)**: First-class change operations (`INSERT`, `UPDATE`, `DELETE`) natively consumed by connectors like Iceberg, Delta Lake, and Spanner.
* **Lifecycle & Observability**: Allow SDK level Pipeline drain support (currently as a Dataflow runner feature).
* **DoFn API Evolution**: Fluent `OutputBuilder` APIs allowing transforms to selectively set metadata without combinatorial method overloads.

Learn more from the [Beam Element Extended Metadata Design Doc](https://s.apache.org/beam-element-extended-metadata).

# OpenTelemetry Support

Distributed tracing is essential for diagnosing latency bottlenecks and tracking record provenance across streaming pipelines. Beam is adding native [OpenTelemetry](https://opentelemetry.io/) support across the Java SDK and connectors ([#33176](https://github.com/apache/beam/issues/33176)):

* **Per-Element Context Propagation**: Leveraging the Extended Metadata framework to propagate W3C Trace Context headers (`traceparent` and `tracestate`) on each element across transform stages, shuffles, and worker boundaries.
* **Connector Header Propagation**: Integrating trace context injection and extraction into key streaming I/Os, including both reads and writes in `KafkaIO` and `PubSubIO`, as well as trace generation for `SpannerIO` change streams.
* **Runner Integration & Log Correlation**: Enabling end-to-end trace propagation through runners (such as Dataflow streaming runner), including stitching OpenTelemetry traces directly with worker logs for unified observability.

# IO Connector Ecosystem

We are actively expanding the Beam I/O connector portfolio with a focus on modern data lakehouse formats, native Change Data Capture (CDC), and unified access patterns:

* **First-Class Lakehouse Connectors**:
  * **[Apache Iceberg](/documentation/io/built-in/iceberg/)**: Full support for batch and streaming reads, streaming appends, dynamic destinations, and changelog CDC reading. Available natively in Java, via cross-language transforms in Python, in [Beam YAML](/documentation/sdks/yaml/), and via the [Managed I/O](/documentation/io/managed-io/) API.
  * **Delta Lake**: Native read capabilities ([#38551](https://github.com/apache/beam/issues/38551)) and batch changelog (CDC) reading ([#39492](https://github.com/apache/beam/issues/39492)), exposed through core transforms and the [Managed I/O](/documentation/io/managed-io/) API.
* **Managed I/O Expansion**: Exposing more storage systems through the [Managed I/O API](/documentation/io/managed-io/), which standardizes connector configurations using Beam Schemas, provides frictionless cross-language access (Java, Python, YAML), and enables runner-level optimizations.
* **Native CDC Integration**: Integrating database change streams (such as Spanner change streams and Debezium-based sources) directly into lakehouse sinks using Beam's native CDC `ValueKind` metadata.

See the full list of available connectors in the [Beam Connectors Overview](/documentation/io/connectors/).
