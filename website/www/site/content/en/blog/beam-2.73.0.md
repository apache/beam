---
title:  "Apache Beam 2.73.0"
date:   2026-04-?? 9:00:00 -0700
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

We are happy to present the new 2.73.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2730-2026-04-??) for this release.

<!--more-->

For more information on changes in 2.73.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/39).

## Highlights

### I/Os

* DebeziumIO (Java): added `OffsetRetainer` interface and `FileSystemOffsetRetainer` implementation to persist and restore CDC offsets across pipeline restarts, and exposed `withStartOffset` / `withOffsetRetainer` on `DebeziumIO.Read` and the cross-language `ReadBuilder` ([#28248](https://github.com/apache/beam/issues/28248)).

### New Features / Improvements

* Added `ADKAgentModelHandler` for running Google Agent Development Kit (ADK) agents (Python) ([#37917](https://github.com/apache/beam/issues/37917)).
* (Python) Added exception chaining to preserve error context in CloudSQLEnrichmentHandler, processes utilities, and core transforms ([#37422](https://github.com/apache/beam/issues/37422)).
* (Python) Added a pipeline option `--experiments=pip_no_build_isolation` to disable build isolation when installing dependencies in the runtime environment ([#37331](https://github.com/apache/beam/issues/37331)).
* (Go) Added OrderedListState support to the Go SDK stateful DoFn API ([#37629](https://github.com/apache/beam/issues/37629)).
* Added support for large pipeline options via a file (Python) ([#37370](https://github.com/apache/beam/issues/37370)).
* Supported infer schema from dataclass (Python) ([#22085](https://github.com/apache/beam/issues/22085)). Default coder for typehint-ed (or set with_output_type) for non-frozen dataclasses changed to RowCoder. To preserve the old behavior (fast primitive coder), explicitly register the type with FastPrimitiveCoder.
* Updates minimum Go version to 1.26.1 ([#37897](https://github.com/apache/beam/issues/37897)).
* (Python) Added image embedding support in `apache_beam.ml.rag` package ([#37628](https://github.com/apache/beam/issues/37628)).
* (Python) Added support for Python version 3.14 ([#37247](https://github.com/apache/beam/issues/37247)).

### Breaking Changes

* The Python SDK container's `boot.go` now passes pipeline options through a file instead of the `PIPELINE_OPTIONS` environment variable. If a user pairs a new Python SDK container with an older SDK version (which does not support the file-based approach), the pipeline options will not be recognized and the pipeline will fail. Users must ensure their SDK and container versions are synchronized ([#37370](https://github.com/apache/beam/issues/37370)).

### Bugfixes

* Fixed ProcessManager not reaping child processes, causing zombie process accumulation on long-running Flink deployments (Java) ([#37930](https://github.com/apache/beam/issues/37930)).

### Security Fixes

* Fixed [CVE-2023-46604](https://www.cve.org/CVERecord?id=CVE-2023-46604) (CVSS 10.0) and [CVE-2022-41678](https://www.cve.org/CVERecord?id=CVE-2022-41678) by upgrading ActiveMQ from 5.14.5 to 5.19.2 (Java) ([#37943](https://github.com/apache/beam/issues/37943)).
* Fixed [CVE-2024-1597](https://www.cve.org/CVERecord?id=CVE-2024-1597), [CVE-2022-31197](https://www.cve.org/CVERecord?id=CVE-2022-31197), and [CVE-2022-21724](https://www.cve.org/CVERecord?id=CVE-2022-21724) by upgrading PostgreSQL JDBC Driver from 42.2.16 to 42.6.2 (Java) ([#37942](https://github.com/apache/beam/issues/37942)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.73.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Alex Malao, Alexander Nieuwenhuijse, Andres Tiko, Andrew Crites, Arun Pandian, Bentsi Leviav, Bruno Volpato, Chamikara Jayalath, Chandra Kiran Bolla, Danny McCormick, Deji Ibrahim, Derrick Williams, Elia LIU, Esmelealem, Hannes Gustafsson, Jack McCluskey, Joey Tran, Kenneth Knowles, M Junaid Shaukat, Mansi Singh, Matej Aleksandrov, Mathijs Deelen, Mattie Fu, Praneet Nadella, Radek Stankiewicz, Radosław Stankiewicz, Reuven Lax, RuiLong J., S. Veyrié, Sakthivel Subramanian, Sam Whittle, Shubham Thakur, Shunping Huang, Subramanya V, Tarun Annapareddy, Tobias Kaymak, Valentyn Tymofieiev, Vitaly Terentyev, XQ Hu, Yi Hu, ZIHAN DAI, claudevdm, kishorepola, parveensania
