---
title:  "Apache Beam 2.67.0"
date:   2025-08-12 15:00:00 -0500
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

We are happy to present the new 2.67.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2670-2025-08-12) for this release.

<!--more-->

For more information on changes in 2.67.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/35?closed=1).

## Highlights

### I/Os

* Debezium IO upgraded to 3.1.1 requires Java 17 (Java) ([#34747](https://github.com/apache/beam/issues/34747)).
* Add support for streaming writes in IOBase (Python)
* Implement support for streaming writes in FileBasedSink (Python)
* Expose support for streaming writes in TextIO (Python)

### New Features / Improvements

* Added support for Processing time Timer in the Spark Classic runner ([#33633](https://github.com/apache/beam/issues/33633)).
* Add pip-based install support for JupyterLab Sidepanel extension ([#35397](https://github.com/apache/beam/issues/35397)).
* [IcebergIO] Create tables with a specified table properties ([#35496](https://github.com/apache/beam/pull/35496))
* Add support for comma-separated options in Python SDK (Python) ([#35580](https://github.com/apache/beam/pull/35580)).
  Python SDK now supports comma-separated values for experiments and dataflow_service_options,
  matching Java SDK behavior while maintaining backward compatibility.
* Milvus enrichment handler added (Python) ([#35216](https://github.com/apache/beam/pull/35216)).
  Beam now supports Milvus enrichment handler capabilities for vector, keyword,
  and hybrid search operations.
* [Beam SQL] Add support for DATABASEs, with an implementation for Iceberg ([#35637](https://github.com/apache/beam/issues/35637))
* Respect BatchSize and MaxBufferingDuration when using `JdbcIO.WriteWithResults`. Previously, these settings were ignored ([#35669](https://github.com/apache/beam/pull/35669)).

### Breaking Changes

* Go: The pubsubio.Read transform now accepts ReadOptions as a value type instead of a pointer, and requires exactly one of Topic or Subscription to be set (they are mutually exclusive). Additionally, the ReadOptions struct now includes a Topic field for specifying the topic directly, replacing the previous topic parameter in the Read function signature ([#35369](https://github.com/apache/beam/pull/35369)).
* SQL: The `ParquetTable` external table provider has changed its handling of the `LOCATION` property. To read from a directory, the path must now end with a trailing slash (e.g., `LOCATION '/path/to/data/'`). Previously, a trailing slash was not required. This change was made to enable support for glob patterns and single-file paths ([#35582](https://github.com/apache/beam/pull/35582)).

### Bugfixes

* [YAML] Fixed handling of missing optional fields in JSON parsing ([#35179](https://github.com/apache/beam/issues/35179)).
* [Python] Fix WriteToBigQuery transform using CopyJob does not work with WRITE_TRUNCATE write disposition ([#34247](https://github.com/apache/beam/issues/34247))
* [Python] Fixed dicomio tags mismatch in integration tests ([#30760](https://github.com/apache/beam/issues/30760)).
* [Java] Fixed spammy logging issues that affected versions 2.64.0 to 2.66.0.

### Known Issues

* ([#35666](https://github.com/apache/beam/issues/35666)). YAML Flatten incorrectly drops fields when input PCollections' schema are different. This issue exists for all versions since 2.52.0.

## List of Contributors

According to git shortlog, the following people contributed to the 2.67.0 release. Thank you to all contributors!

Aditya Shukla, Ahmed Abualsaud, Arun Pandian, Boris Li, Chamikara Jayalath, Charles Nguyen, Chenzo, Danny McCormick, David Adeniji, Derrick Williams, Dmytro Tsyliuryk, Dustin Rhodes, Enrique Calderon, Gottipati Gautam, Hai Joey Tran, Hunor Portik, Jack McCluskey, Kenneth Knowles, Khorbaladze A., Marcio Sugar, Minh Son Nguyen, Mohamed Awnallah, Nathaniel Young, Nhon Dinh, Quentin Sommer, Rafael Raposo, Rakesh Kumar, Razvan Culea, Reuven Lax, Robert Bradshaw, Sam Whittle, Shunping Huang, Steven van Rossum, Talat UYARER, Tanu Sharma, Tarun Annapareddy, Tobi Kaymak, Tobias Kaymak, Valentyn Tymofieiev, Veronica Wasson, Vitaly Terentyev, XQ Hu, Yi Hu, akashorabek, arnavarora2004, changliiu, claudevdm, fozzie15, mvhensbergen, twosom
