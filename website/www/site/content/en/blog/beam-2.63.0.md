---
title:  "Apache Beam 2.63.0"
date:   2025-02-18 10:30:00 -0500
categories:
  - blog
  - release
authors:
  - jrmccluskey
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

We are happy to present the new 2.63.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/{$DOWNLOAD_ANCHOR}) for this release.

<!--more-->

For more information on changes in 2.63.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/27).

## I/Os

* Support gcs-connector 3.x+ in GcsUtil ([#33368](https://github.com/apache/beam/pull/33368))
* Support for X source added (Java/Python) ([#X](https://github.com/apache/beam/issues/X)).
* Introduced `--groupFilesFileLoad` pipeline option to mitigate side-input related issues in BigQueryIO
  batch FILE_LOAD on certain runners (including Dataflow Runner V2) (Java) ([#33587](https://github.com/apache/beam/pull/33587)).

## New Features / Improvements

* Add BigQuery vector/embedding ingestion and enrichment components to apache_beam.ml.rag (Python) ([#33413](https://github.com/apache/beam/pull/33413)).
* Upgraded to protobuf 4 (Java) ([#33192](https://github.com/apache/beam/issues/33192)).
* [GCSIO] Added retry logic to each batch method of the GCS IO (Python) ([#33539](https://github.com/apache/beam/pull/33539))
* [GCSIO] Enable recursive deletion for GCSFileSystem Paths (Python) ([#33611](https://github.com/apache/beam/pull/33611)).
* External, Process based Worker Pool support added to the Go SDK container. ([#33572](https://github.com/apache/beam/pull/33572))
  * This is used to enable sidecar containers to run SDK workers for some runners.
  * See https://beam.apache.org/documentation/runtime/sdk-harness-config/ for details.
* Support the Process Environment for execution in the Go SDK. ([#33651](https://github.com/apache/beam/pull/33651))
* Prism
  * Prism now uses the same single port for both pipeline submission and execution on workers. Requests are differentiated by worker-id. ([#33438](https://github.com/apache/beam/pull/33438))
    * This avoids port starvation and provides clarity on port use when running Prism in non-local environments.
  * Support for @RequiresTimeSortedInputs added. ([#33513](https://github.com/apache/beam/issues/33513))
  * Initial support for AllowedLateness added. ([#33542](https://github.com/apache/beam/pull/33542))
  * The Go SDK's inprocess Prism runner (AKA the Go SDK default runner) now supports non-loopback mode environment types. ([#33572](https://github.com/apache/beam/pull/33572))
  * Support the Process Environment for execution in Prism ([#33651](https://github.com/apache/beam/pull/33651))
  * Support the AnyOf Environment for execution in Prism ([#33705](https://github.com/apache/beam/pull/33705))
     * This improves support for developing Xlang pipelines, when using a compatible cross language service.
* Partitions are now configurable for the DaskRunner in the Python SDK ([#33805](https://github.com/apache/beam/pull/33805)).
* [Dataflow Streaming] Enable Windmill GetWork Response Batching by default ([#33847](https://github.com/apache/beam/pull/33847)).
  * With this change user workers will request batched GetWork responses from backend and backend will send multiple WorkItems in the same response proto.
  * The feature can be disabled by passing `--windmillRequestBatchedGetWorkResponse=false`

## Breaking Changes

* AWS V1 I/Os have been removed (Java). As part of this, x-lang Python Kinesis I/O has been updated to consume the V2 IO and it also no longer supports setting producer_properties ([#33430](https://github.com/apache/beam/issues/33430)).
* Upgraded to protobuf 4 (Java) ([#33192](https://github.com/apache/beam/issues/33192)), but forced Debezium IO to use protobuf 3 ([#33541](https://github.com/apache/beam/issues/33541) because Debezium clients are not protobuf 4 compatible. This may cause conflicts when using clients which are only compatible with protobuf 4.
* Minimum Go version for Beam Go updated to 1.22.10 ([#33609](https://github.com/apache/beam/pull/33609))

## Bugfixes

* Fix data loss issues when reading gzipped files with TextIO (Python) ([#18390](https://github.com/apache/beam/issues/18390), [#31040](https://github.com/apache/beam/issues/31040)).
* [BigQueryIO] Fixed an issue where Storage Write API sometimes doesn't pick up auto-schema updates ([#33231](https://github.com/apache/beam/pull/33231))
* Prism
  * Fixed an edge case where Bundle Finalization might not become enabled. ([#33493](https://github.com/apache/beam/issues/33493)).
  * Fixed session window aggregation, which wasn't being performed per-key. ([#33542](https://github.com/apache/beam/issues/33542)).)
* [Dataflow Streaming Appliance] Fixed commits failing with KeyCommitTooLargeException when a key outputs >180MB of results. [#33588](https://github.com/apache/beam/issues/33588).
* Fixed a Dataflow template creation issue that ignores template file creation errors (Java) ([#33636](https://github.com/apache/beam/issues/33636))
* Correctly documented Pane Encodings in the portability protocols ([#33840](https://github.com/apache/beam/issues/33840)).
* Fixed the user mailing list address ([#26013](https://github.com/apache/beam/issues/26013)).
* [Dataflow Streaming] Fixed an issue where Dataflow Streaming workers were reporting lineage metrics as cumulative rather than delta. ([#33691](https://github.com/apache/beam/pull/33691))

## Known Issues

* (Java) Current version of protobuf has a [bug](https://github.com/protocolbuffers/protobuf/issues/20599) leading to incompatibilities with clients using older versions of Protobuf ([example issue](https://github.com/GoogleCloudPlatform/DataflowTemplates/issues/2191)). This issue has been seen in SpannerIO in particular. Tracked in [#34452](https://github.com/GoogleCloudPlatform/DataflowTemplates/issues/34452)

## List of Contributors

According to git shortlog, the following people contributed to the 2.63.0 release. Thank you to all contributors!

Ahmed Abualsaud,
Alex Merose,
Andrej Galad,
Andrew Crites,
Arun Pandian,
Bartosz Zablocki,
Chamikara Jayalath,
Claire McGinty,
Clay Johnson,
Damon Douglas,
Danish Amjad,
Danny McCormick,
Deep1998,
Derrick Williams,
Dmitry Labutin,
Dmytro Sadovnychyi,
Eduardo Ramírez,
Filipe Regadas,
Hai Joey Tran,
Jack McCluskey,
Jan Lukavský,
Jeff Kinard,
Jozef Vilcek,
Julien Tournay,
Kenneth Knowles,
Michel Davit,
Miguel Trigueira,
Minbo Bae,
Mohamed Awnallah,
Mohit Paddhariya,
Nahian-Al Hasan,
Naireen Hussain,
Niall Pemberton,
Radosław Stankiewicz,
Razvan Culea,
Robert Bradshaw,
Robert Burke,
Rohit Sinha,
S. Veyrié,
Sam Whittle,
Sergei Lilichenko,
Shingo Furuyama,
Shunping Huang,
Thiago Nunes,
Tim Heckman,
Tobias Bredow,
Tom Stepp,
Tony Tang,
VISHESH TRIPATHI,
Vitaly Terentyev,
Yi Hu,
XQ Hu,
akashorabek,
claudevdm
