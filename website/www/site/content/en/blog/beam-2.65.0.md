---
title:  "Apache Beam 2.65.0"
date:   2025-05-12 15:00:00 -0500
categories:
  - blog
  - release
authors:
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

We are happy to present the new 2.65.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2650-2025-05-12) for this release.

<!--more-->

For more information on changes in 2.65.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/29?closed=1).

## Highlights

### I/Os

* Upgraded GoogleAdsAPI to v19 for GoogleAdsIO (Java) ([#34497](https://github.com/apache/beam/pull/34497)). Changed PTransform method from version-specified (`v17()`) to `current()` for better backward compatibility in the future.
* Added support for writing to Pubsub with ordering keys (Java) ([#21162](https://github.com/apache/beam/issues/21162))

### New Features / Improvements

* Added support for streaming side-inputs in the Spark Classic runner ([#18136](https://github.com/apache/beam/issues/18136)).

### Breaking Changes

* [Python] Cloudpickle is set as the default `pickle_library`, where previously
  dill was the default in [#34695](https://github.com/apache/beam/pull/34695).
  For known issues, reporting new issues, and understanding cloudpickle
  behavior refer to [#34903](https://github.com/apache/beam/issues/34903).
* [Python] Reshuffle now preserves PaneInfo, where previously PaneInfo was lost
  after reshuffle. To opt out of this change, set the
  update_compatibility_version to a previous Beam version e.g. "2.64.0".
([#34348](https://github.com/apache/beam/pull/34348)).
* [Python] PaneInfo is encoded by PaneInfoCoder, where previously PaneInfo was
  encoded with FastPrimitivesCoder falling back to PickleCoder. This only
  affects cases where PaneInfo is directly stored as an element.
  ([#34824](https://github.com/apache/beam/pull/34824)).
* [Python] BigQueryFileLoads now adds a Reshuffle before triggering load jobs.
  This fixes a bug where there can be data loss in a streaming pipeline if there
  is a pending load job during autoscaling. To opt out of this change, set the
  update_compatibility_version to a previous Beam version e.g. "2.64.0".
([#34657](https://github.com/apache/beam/pull/34657))
* [YAML] Kafka source and sink will be automatically replaced with compatible managed transforms.
  For older Beam versions, streaming update compatiblity can be maintained by specifying the pipeline
  option `update_compatibility_version` ([#34767](https://github.com/apache/beam/issues/34767)).


### Deprecations

* Beam ZetaSQL is deprecated and will be removed no earlier than Beam 2.68.0 ([#34423](https://github.com/apache/beam/issues/34423)).
  Users are recommended to switch to [Calcite SQL](https://beam.apache.org/documentation/dsls/sql/calcite/overview/) dialect.

### Bugfixes

* Fixed read Beam rows from cross-lang transform (for example, ReadFromJdbc) involving negative 32-bit integers incorrectly decoded to large integers ([#34089](https://github.com/apache/beam/issues/34089))
* (Java) Fixed SDF-based KafkaIO (ReadFromKafkaViaSDF) to properly handle custom deserializers that extend Deserializer<Row> interface([#34505](https://github.com/apache/beam/pull/34505))
* [Python] `TypedDict` typehints are now compatible with `Mapping` and `Dict` type annotations.

### Security Fixes

* Fixed [CVE-2025-30065](https://www.cve.org/CVERecord?id=CVE-2025-30065) (Java) ([#34573](https://github.com/apache/beam/pull/34573))

### Known Issues

N/A

## List of Contributors

According to git shortlog, the following people contributed to the 2.65.0 release. Thank you to all contributors!

Aaron Trelstad, Adrian Stoll, Ahmed Abualsaud, akashorabek, Arun Pandian, Bentsi Leviav, Bryan Dang, Celeste Zeng, Chamikara Jayalath, claudevdm, Danny McCormick, Derrick Williams, Ozzie Fernandez, Gabija Balvociute, Gayatri Kate, illoise, Jack McCluskey, Jan Lukavský, Jinho Lee, Justin Bandoro, Kenneth Knowles, XQ Hu, Luke Tsekouras, Martin Trieu, Matthew Suozzo, Naireen Hussain, Niel Markwick, Radosław Stankiewicz, Razvan Culea, Robert Bradshaw, Robert Burke, RuiLong J., Sam Whittle, Sarthak, Shubham Jaiswal, Shunping Huang, Steven van Rossum, Suvrat Acharya, sveyrie@luminatedata.com, Talat Uyarer, TanuSharma2511, Tobias Kaymak, Tom Stepp, Valentyn Tymofieiev, twosom, Vitaly Terentyev, wollowizard, Yi Hu, Yifan Ye, Zilin Du
