---
title:  "Apache Beam 2.75.0"
date:   2026-07-?? 14:00:00 -0500
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

We are happy to present the new 2.75.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2750-2026-07-??) for this release.

<!--more-->

For more information on changes in 2.75.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/43).

## Highlights

* Python SDK now supports memory profiling with Memray ([#38853](https://github.com/apache/beam/issues/38853)).
* (Python) Added [Qdrant](https://qdrant.tech/) VectorDatabaseWriteConfig implementation ([#38141](https://github.com/apache/beam/issues/38141)).

### I/Os

* Support for reading from Delta Lake added (Java) ([#38551](https://github.com/apache/beam/issues/38551)).
* ClickHouseIO: support writing `DateTime64(precision[, 'timezone'])` columns with sub-second precision (Java) ([#38466](https://github.com/apache/beam/issues/38466)).
* Upgraded IO Expansion Service to Java 17 ([#38974](https://github.com/apache/beam/issues/38974)).

### New Features / Improvements

* Dataflow Runner v2 has been renamed to Dataflow Portable Runner. Please refer to Dataflow [public documentation](https://docs.cloud.google.com/dataflow/docs/runner-v2) on when to enable Portable Runner.([#39000](https://github.com/apache/beam/issues/39000)).
* (Java) Enabled state tag encoding v2 by default for new Dataflow Streaming Engine jobs. It can be disabled by passing `--experiments=disable_streaming_engine_state_tag_encoding_v2` or `--updateCompatibilityVersion=2.74.0` pipeline option. Note that the tag encoding version cannot change during a job update. Jobs using tag encoding v2 (enabled by default for new jobs on 2.75.0+) cannot be downgraded to Beam versions prior to 2.73.0, as only versions 2.73.0 and later support tag encoding v2. ([#38705](https://github.com/apache/beam/issues/38705)).
* (Python) Added instrumentation to support off-the-shelf profiling agents when launching Python SDK Harness ([#38853](https://github.com/apache/beam/issues/38853)).
* (Java) Added support to the FnApi Data stream protocol allowing runners to isolate bundles slowly processing input from other bundles. ([#39001](https://github.com/apache/beam/issues/39001)).
* (YAML) Switched js2py library to Quickjs ([#38473](https://github.com/apache/beam/issues/38473)).
* (YAML) Added HuggingFaceModelHandler for YAML usage ([#38696](https://github.com/apache/beam/issues/38696)).
* (YAML) Added WriteToMongoDB transform ([#38376](https://github.com/apache/beam/issues/38376)).
* (YAML) Added WriteToDatadog transform ([#38362](https://github.com/apache/beam/issues/38362)).
* (Java) Flink 2.1 and 2.2 support is added ([#38947](https://github.com/apache/beam/issues/38947)) ([#38978](https://github.com/apache/beam/issues/38978)); Flink 1.17 and 1.18 support is dropped.
* (Python) MqttIO is now supported in Python via cross-language ([#21060](https://github.com/apache/beam/issues/21060)).

### Breaking Changes

* (Python) Typehints of dataclass fields are honored during type inferences. To restore the behavior of fallback-to-any,
  use pipeline option `--exclude_infer_dataclass_field_type` ([#38797](https://github.com/apache/beam/issues/38797)).
  However fixing forward is recommended.

### Bugfixes

* Fixed GCS filesystem glob matching to correctly handle `/` in object names and support `**` for recursive matching (Go) ([#38059](https://github.com/apache/beam/issues/38059)).
* Fixed BigQueryEnrichmentHandler batch mode dropping earlier requests when multiple requests share the same enrichment key (Python) ([#38035](https://github.com/apache/beam/issues/38035)).
* Fixed IcebergIO writing manifest column bounds padded with trailing `0x00` bytes, which broke equality predicate pushdown in some query engines (Java) ([#38580](https://github.com/apache/beam/issues/38580)).

### Known Issues

* (Java) Projects using the Flink runner with Flink 2.1 or later alongside libraries requiring `org.lz4:lz4-java` (e.g., Kafka clients) may encounter a Gradle capability conflict, because Flink 2.1+ ships `at.yawk.lz4:lz4-java` which declares the same capability. To resolve, add a `capabilitiesResolution` rule to your `build.gradle` that selects `at.yawk.lz4:lz4-java` ([#38947](https://github.com/apache/beam/issues/38947)).

According to git shortlog, the following people contributed to the 2.75.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Akshat, Andrew Crites, Andrew Kabas, Anurag Pappula, Arpit Jain, Arun Pandian, Atharv, Chamikara Jayalath, Danny McCormick, Deji Ibrahim, Derrick Williams, Drew Stevens, Durgaprasad M L, Elia Liu, Enzo Maruffa Moreira, Ganesh Sivakumar, Goutam Adwant, HansMarcus01, Jack McCluskey, Joe Santos, Kenneth Knowles, Lalit Yadav, Liam Miller-Cushon, Maciej Szwaja, Manan Mangal, Michael Gruschke, Nikita Grover, Radek Stankiewicz, Radosław Stankiewicz, Reuven Lax, RuiLong J., Sachin Ranjalkar, Sagnik Ghosh, Sam Whittle, Shunping Huang, Subramanya V, Tarun Annapareddy, Tobias Kaymak, TongruiLi, Valentyn Tymofieiev, Vitaly Terentyev, XQ Hu, Yi Hu, aaaZayne, claudevdm, ddebowczyk92, innuendo, kellen, parveensania, tejasiyer-dev
