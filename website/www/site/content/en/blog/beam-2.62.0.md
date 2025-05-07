---
title:  "Apache Beam 2.62.0"
date:   2025-01-21 10:30:00 -0500
categories:
  - blog
  - release
authors:
  - klk
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

We are happy to present the new 2.62.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/{$DOWNLOAD_ANCHOR}) for this release.

<!--more-->

For more information on changes in 2.62.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/26).

## New Features / Improvements

* Added support for stateful processing in Spark Runner for streaming pipelines. Timer functionality is not yet supported and will be implemented in a future release ([#33237](https://github.com/apache/beam/issues/33237)).
* The datetime module is now available for use in jinja templatization for yaml.
* Improved batch performance of SparkRunner's GroupByKey ([#20943](https://github.com/apache/beam/pull/20943)).
* Support OnWindowExpiration in Prism ([#32211](https://github.com/apache/beam/issues/32211)).
  * This enables initial Java GroupIntoBatches support.
* Support OrderedListState in Prism ([#32929](https://github.com/apache/beam/issues/32929)).

## I/Os

* gcs-connector config options can be set via GcsOptions (Java) ([#32769](https://github.com/apache/beam/pull/32769)).
* [Managed Iceberg] Support partitioning by time (year, month, day, hour) for types `date`, `time`, `timestamp`, and `timestamp(tz)` ([#32939](https://github.com/apache/beam/pull/32939))
* Upgraded the default version of Hadoop dependencies to 3.4.1. Hadoop 2.10.2 is still supported (Java) ([#33011](https://github.com/apache/beam/issues/33011)).
* [BigQueryIO] Create managed BigLake tables dynamically ([#33125](https://github.com/apache/beam/pull/33125))

## Breaking Changes

* Upgraded ZetaSQL to 2024.11.1 ([#32902](https://github.com/apache/beam/pull/32902)). Java11+ is now needed if Beam's ZetaSQL component is used.

## Bugfixes

* Fixed EventTimeTimer ordering in Prism. ([#32222](https://github.com/apache/beam/issues/32222)).
* [Managed Iceberg] Fixed a bug where DataFile metadata was assigned incorrect partition values ([#33549](https://github.com/apache/beam/pull/33549)).

## Security Fixes

* Fixed [CVE-2024-47561](https://www.cve.org/CVERecord?id=CVE-2024-47561) (Java) by upgrading Avro version to 1.11.4

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## Known Issues

* [Python] If you are using the official Apache Beam Python containers for version 2.62.0, be aware that they include NumPy version 1.26.4. It is strongly recommended that you explicitly specify numpy==1.26.4 in your project's dependency list. ([#33639](https://github.com/apache/beam/issues/33639)).
* [Dataflow Streaming Appliance] Commits fail with KeyCommitTooLargeException when a key outputs >180MB of results. Bug affects versions 2.60.0 to 2.62.0,
  * fix will be released with 2.63.0. [#33588](https://github.com/apache/beam/issues/33588).
  * To resolve this issue, downgrade to 2.59.0 or upgrade to 2.63.0 or enable [Streaming Engine](https://cloud.google.com/dataflow/docs/streaming-engine#use).

## List of Contributors

According to git shortlog, the following people contributed to the 2.62.0 release. Thank you to all contributors!

Ahmed Abualsaud,
Ahmet Altay,
Alex Merose,
Andrew Crites,
Arnout Engelen,
Attila Doroszlai,
Bartosz Zablocki,
Chamikara Jayalath,
Claire McGinty,
Claude van der Merwe,
Damon Douglas,
Danny McCormick,
Gabija Balvociute,
Hai Joey Tran,
Hakampreet Singh Pandher,
Ian Sullivan,
Jack McCluskey,
Jan Lukavský,
Jeff Kinard,
Jeffrey Kinard,
Laura Detmer,
Kenneth Knowles,
Martin Trieu,
Mattie Fu,
Michel Davit,
Naireen Hussain,
Nick Anikin,
Radosław Stankiewicz,
Ravi Magham,
Reeba Qureshi,
Robert Bradshaw,
Robert Burke,
Rohit Sinha,
S. Veyrié,
Sam Whittle,
Shingo Furuyama,
Shunping Huang,
Svetak Sundhar,
Valentyn Tymofieiev,
Vlado Djerek,
XQ Hu,
Yi Hu,
twosom
