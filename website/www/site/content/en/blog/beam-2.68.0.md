---
title:  "Apache Beam 2.68.0"
date:   2025-09-22 15:00:00 -0500
categories:
  - blog
  - release
authors:
  - vterentev
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

We are happy to present the new 2.68.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2680-2025-09-??) for this release.

<!--more-->

For more information on changes in 2.68.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/36?closed=1).

## Highlights

* [Python] Prism runner now enabled by default for most Python pipelines using the direct runner ([#34612](https://github.com/apache/beam/pull/34612)). This may break some tests, see https://github.com/apache/beam/pull/34612 for details on how to handle issues.

### I/Os

* Upgraded Iceberg dependency to 1.9.2 ([#35981](https://github.com/apache/beam/pull/35981))

### New Features / Improvements

* BigtableRead Connector for BeamYaml added with new Config Param ([#35696](https://github.com/apache/beam/pull/35696))
* MongoDB Java driver upgraded from 3.12.11 to 5.5.0 with API refactoring and GridFS implementation updates (Java) ([#35946](https://github.com/apache/beam/pull/35946)).
* Introduced a dedicated module for JUnit-based testing support: `sdks/java/testing/junit`, which provides `TestPipelineExtension` for JUnit 5 while maintaining backward compatibility with existing JUnit 4 `TestRule`-based tests (Java) ([#18733](https://github.com/apache/beam/issues/18733), [#35688](https://github.com/apache/beam/pull/35688)).
  - To use JUnit 5 with Beam tests, add a test-scoped dependency on `org.apache.beam:beam-sdks-java-testing-junit`.
* Google CloudSQL enrichment handler added (Python) ([#34398](https://github.com/apache/beam/pull/34398)).
  Beam now supports data enrichment capabilities using SQL databases, with built-in support for:
  - Managed PostgreSQL, MySQL, and Microsoft SQL Server instances on CloudSQL
  - Unmanaged SQL database instances not hosted on CloudSQL (e.g., self-hosted or on-premises databases)
* [Python] Added the `ReactiveThrottler` and `ThrottlingSignaler` classes to streamline throttling behavior in DoFns, expose throttling mechanisms for users ([#35984](https://github.com/apache/beam/pull/35984))
* Added a pipeline option to specify the processing timeout for a single element by any PTransform (Java/Python/Go) ([#35174](https://github.com/apache/beam/issues/35174)).
  - When specified, the SDK harness automatically restarts if an element takes too long to process. Beam runner may then retry processing of the same work item.
  - Use the `--element_processing_timeout_minutes` option to reduce the chance of having stalled pipelines due to unexpected cases of slow processing, where slowness might not happen again if processing of the same element is retried.
* (Python) Adding GCP Spanner Change Stream support for Python (apache_beam.io.gcp.spanner) ([#24103](https://github.com/apache/beam/issues/24103)).

### Breaking Changes

* Previously deprecated Beam ZetaSQL component has been removed ([#34423](https://github.com/apache/beam/issues/34423)).
  ZetaSQL users could migrate to Calcite SQL with BigQuery dialect enabled.
* Upgraded Beam vendored Calcite to 1.40.0 for Beam SQL ([#35483](https://github.com/apache/beam/issues/35483)), which
  improves support for BigQuery and other SQL dialects. Note: Minor behavior changes are observed such as output
  significant digits related to casting.
* (Python) The deterministic fallback coder for complex types like NamedTuple, Enum, and dataclasses now uses cloudpickle instead of dill. If your pipeline is affected, you may see a warning like: "Using fallback deterministic coder for type X...". You can revert to the previous behavior by using the pipeline option `--update_compatibility_version=2.67.0` ([35725](https://github.com/apache/beam/pull/35725)). Report any pickling related issues to [#34903](https://github.com/apache/beam/issues/34903)
* (Python) Prism runner now enabled by default for most Python pipelines using the direct runner ([#34612](https://github.com/apache/beam/pull/34612)). This may break some tests, see https://github.com/apache/beam/pull/34612 for details on how to handle issues.
* Dropped Java 8 support for [IO expansion-service](https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-io-expansion-service). Cross-language pipelines using this expansion service will need a Java11+ runtime ([#35981](https://github.com/apache/beam/pull/35981).

### Deprecations

* Python SDK native SpannerIO (apache_beam/io/gcp/experimental/spannerio) is deprecated. Use cross-language wrapper
  (apache_beam/io/gcp/spanner) instead (Python) ([#35860](https://github.com/apache/beam/issues/35860)).
* Samza runner is deprecated and scheduled for removal in Beam 3.0 ([#35448](https://github.com/apache/beam/issues/35448)).
* Twister2 runner is deprecated and scheduled for removal in Beam 3.0 ([#35905](https://github.com/apache/beam/issues/35905))).

### Bugfixes

* (Python) Fixed Java YAML provider fails on Windows ([#35617](https://github.com/apache/beam/issues/35617)).
* Fixed BigQueryIO creating temporary datasets in wrong project when temp_dataset is specified with a different project than the pipeline project. For some jobs, temporary datasets will now be created in the correct project (Python) ([#35813](https://github.com/apache/beam/issues/35813)).
* (Go) Fix duplicates due to reads after blind writes to Bag State ([#35869](https://github.com/apache/beam/issues/35869)).
  * Earlier Go SDK versions can avoid the issue by not reading in the same call after a blind write.

## List of Contributors

According to git shortlog, the following people contributed to the 2.68.0 release. Thank you to all contributors!

Ahmed Abualsaud, Andrew Crites, Ashok Devireddy, Chamikara Jayalath, Charles Nguyen, Danny McCormick, Davda James, Derrick Williams, Diego Hernandez, Dip Patel, Dustin Rhodes, Enrique Calderon, Hai Joey Tran, Jack McCluskey, Kenneth Knowles, Keshav, Khorbaladze A., LEEKYE, Lanny Boarts, Mattie Fu, Minbo Bae, Mohamed Awnallah, Naireen Hussain, Nathaniel Young, Radosław Stankiewicz, Razvan Culea, Robert Bradshaw, Robert Burke, Sam Whittle, Shehab, Shingo Furuyama, Shunping Huang, Steven van Rossum, Suvrat Acharya, Svetak Sundhar, Tarun Annapareddy, Tom Stepp, Valentyn Tymofieiev, Vitaly Terentyev, XQ Hu, Yi Hu, apanich, arnavarora2004, claudevdm, flpablo, kristynsmith, shreyakhajanchi
