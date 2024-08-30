---
title:  "Apache Beam 2.54.0"
date:   2024-02-14 09:00:00 -0400
categories:
  - blog
  - release
authors:
  - lostluck
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

We are happy to present the new 2.54.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/) for this release.

<!--more-->

For more information on changes in 2.54.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/18).

## Highlights

* [Enrichment Transform](https://s.apache.org/enrichment-transform) along with GCP BigTable handler added to Python SDK ([#30001](https://github.com/apache/beam/pull/30001)).
* Beam Java Batch pipelines run on Google Cloud Dataflow will default to the Portable Runner (v2) starting with this version. (All other languages are already on Runner V2.) See [Runner V2 documentation](https://cloud.google.com/dataflow/docs/runner-v2) for how to enable or disable it intentionally.

## I/Os

* Added support for writing to BigQuery dynamic destinations with Python's Storage Write API ([#30045](https://github.com/apache/beam/pull/30045))
* Adding support for Tuples DataType in ClickHouse (Java) ([#29715](https://github.com/apache/beam/pull/29715)).
* Added support for handling bad records to FileIO, TextIO, AvroIO ([#29670](https://github.com/apache/beam/pull/29670)).
* Added support for handling bad records to BigtableIO ([#29885](https://github.com/apache/beam/pull/29885)).

## New Features / Improvements

* [Enrichment Transform](https://s.apache.org/enrichment-transform) along with GCP BigTable handler added to Python SDK ([#30001](https://github.com/apache/beam/pull/30001)).

## Breaking Changes

* N/A

## Deprecations

* N/A

## Bugfixes

* Fixed a memory leak affecting some Go SDK since 2.46.0. ([#28142](https://github.com/apache/beam/pull/28142))

## Security Fixes

* N/A

## Known Issues

* Some Python pipelines that run with 2.52.0-2.54.0 SDKs and use large materialized side inputs might be affected by a performance regression. To restore the prior behavior on these SDK versions, supply the `--max_cache_memory_usage_mb=0` pipeline option. ([#30360](https://github.com/apache/beam/issues/30360)).
* Python pipelines that run with 2.53.0-2.54.0 SDKs and perform file operations on GCS might be affected by excess HTTP requests. This could lead to a performance regression or a permission issue. ([#28398](https://github.com/apache/beam/issues/28398))
* In Python pipelines, when shutting down inactive bundle processors, shutdown logic can overaggressively hold the lock, blocking acceptance of new work. Symptoms of this issue include slowness or stuckness in long-running jobs. Fixed in 2.56.0 ([#30679](https://github.com/apache/beam/pull/30679)).
* Python pipelines that run with 2.53.0-2.58.0 SDKs and read data from GCS might be affected by a data corruption issue ([#32169](https://github.com/apache/beam/issues/32169)). The issue will be fixed in 2.59.0 ([#32135](https://github.com/apache/beam/pull/32135)). To work around this, update the google-cloud-storage package to version 2.18.2 or newer.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.54.0 release. Thank you to all contributors!

Ahmed Abualsaud

Alexey Romanenko

Anand Inguva

Andrew Crites

Arun Pandian

Bruno Volpato

caneff

Chamikara Jayalath

Changyu Li

Cheskel Twersky

Claire McGinty

clmccart

Damon

Danny McCormick

dependabot[bot]

Edward Cheng

Ferran Fernández Garrido

Hai Joey Tran

hugo-syn

Issac

Jack McCluskey

Jan Lukavský

JayajP

Jeffrey Kinard

Jerry Wang

Jing

Joey Tran

johnjcasey

Kenneth Knowles

Knut Olav Løite

liferoad

Marc

Mark Zitnik

martin trieu

Mattie Fu

Naireen Hussain

Neeraj Bansal

Niel Markwick

Oleh Borysevych

pablo rodriguez defino

Rebecca Szper

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Sam Whittle

Shunping Huang

Svetak Sundhar

S. Veyrié

Talat UYARER

tvalentyn

Vlado Djerek

Yi Hu

Zechen Jian
