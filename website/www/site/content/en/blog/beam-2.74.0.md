---
title:  "Apache Beam 2.74.0"
date:   2026-06-02 14:00:00 -0500
categories:
  - blog
  - release
authors:
  - vterentev
  - yhu
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

We are happy to present the new 2.74.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2740-2026-06-02) for this release.

<!--more-->

For more information on changes in 2.74.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/42).

## Highlights

* Spark 4 runner support for Java SDK ([#38255](https://github.com/apache/beam/issues/38255)).

### I/Os

* IcebergIO: support declaring a table's sort order on dynamic table creation via the new `sort_fields` config ([#38269](https://github.com/apache/beam/issues/38269)).
* IcebergIO: support writing with hash distribution mode, and with autosharding ([#38061](https://github.com/apache/beam/issues/38061)).

### New Features / Improvements

* Capability introduces an indicator for aggregations and timers firing during a pipeline drain, allowing users and sinks to recognize and appropriately handle potentially incomplete or partial data ([#36884](https://github.com/apache/beam/issues/36884)).
* Added support for setting disk provisioned IOPS and throughput in Dataflow runner via `--diskProvisionedIops` and `--diskProvisionedThroughputMibps` pipeline options (Java/Go/Python) ([#38349](https://github.com/apache/beam/issues/38349)).
* TriggerStateMachineRunner changes from BitSetCoder to SentinelBitSetCoder to
  encode finished bitset. SentinelBitSetCoder and BitSetCoder are state
  compatible. Both coders can decode encoded bytes from the other coder
  ([#38139](https://github.com/apache/beam/issues/38139)).
* (Python) Added type alias for with_exception_handling to be used for typehints. ([#38173](https://github.com/apache/beam/issues/38173)).
* (Java) BatchElements transform for Java SDK ([#38369](https://github.com/apache/beam/issues/38369))
* Added plugin mechanism to support different Lineage implementations (Java) ([#36790](https://github.com/apache/beam/issues/36790)).
* (Python) Supported Python user type in Beam SQL. For example, SQL statements like `SELECT some_field from PCOLLECTION` can now operate a PCollection of Beam Row containing pickable Python user type ([#20738](https://github.com/apache/beam/issues/20738)).
* (Python) Introduced `beam.coders.registry.register_row` as preferred API to register a named tuple or dataclass with a Beam Row. At pipelne runtime, the original type associated with the registered row are preserved across the serialization boundary ([#38108](https://github.com/apache/beam/issues/38108)).

### Breaking Changes

* (Python) Made Beartype the default fallback type checking tool. This can be disabled with the `--disable_beartype` pipeline option. ([#38275](https://github.com/apache/beam/issues/38275))

### Deprecations

* Dropped Java 8 support ([#31678](https://github.com/apache/beam/issues/31678)).
* Removed Samza Runner support ([#35448](https://github.com/apache/beam/issues/35448)).

### Bugfixes

* Fixed BigQueryEnrichmentHandler batch mode dropping earlier requests when multiple requests share the same enrichment key (Python) ([#38035](https://github.com/apache/beam/issues/38035)).
* Added `max_batch_duration_secs` passthrough support in Python Enrichment BigQuery and CloudSQL handlers so batching duration can be forwarded to `BatchElements` ([#38243](https://github.com/apache/beam/issues/38243)).

According to git shortlog, the following people contributed to the 2.74.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Andrew Crites, Andrew Kabas, Arran Cudbard-Bell, Arun Pandian, Asish Kumar, Bentsi Leviav, Blake Jones, Bruno Volpato, Chris Jordan, Danny McCormick, Deji Ibrahim, Derrick Williams, Elia LIU, Ganesh Sivakumar, Jack McCluskey, Kenneth Knowles, Lalit Yadav, M Junaid Shaukat, Matej Aleksandrov, Prabhnoor Singh, Radek Stankiewicz, Radosław Stankiewicz, Reuven Lax, RuiLong J., Sam Whittle, Shunping Huang, Subramanya V, Tarun Annapareddy, Tobias Kaymak, TongruiLi, Valentyn Tymofieiev, Vitaly Terentyev, XQ Hu, Yi Hu, ZIHAN DAI, apanich, bambadiouf1, chenxuesdu, claudevdm, harshadkhetpal, johnjcasey, parveensania, tianz101
