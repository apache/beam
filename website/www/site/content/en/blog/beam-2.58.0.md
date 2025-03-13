---
title:  "Apache Beam 2.58.0"
date:   2024-08-06 13:00:00 -0800
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

We are happy to present the new 2.58.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2580-2024-08-06) for this release.

<!--more-->

For more information about changes in 2.58.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/22).

## I/Os

* Support for [Solace](https://solace.com/) source (`SolaceIO.Read`) added (Java) ([#31440](https://github.com/apache/beam/issues/31440)).

## New Features / Improvements

* Multiple RunInference instances can now share the same model instance by setting the model_identifier parameter (Python) ([#31665](https://github.com/apache/beam/issues/31665)).
* Added options to control the number of Storage API multiplexing connections ([#31721](https://github.com/apache/beam/pull/31721))
* [BigQueryIO] Better handling for batch Storage Write API when it hits AppendRows throughput quota ([#31837](https://github.com/apache/beam/pull/31837))
* [IcebergIO] All specified catalog properties are passed through to the connector ([#31726](https://github.com/apache/beam/pull/31726))
* Removed a third-party LGPL dependency from the Go SDK ([#31765](https://github.com/apache/beam/issues/31765)).
* Support for `MapState` and `SetState` when using Dataflow Runner v1 with Streaming Engine (Java) ([[#18200](https://github.com/apache/beam/issues/18200)])

## Breaking Changes

* [IcebergIO] `IcebergCatalogConfig` was changed to support specifying catalog properties in a key-store fashion ([#31726](https://github.com/apache/beam/pull/31726))
* [SpannerIO] Added validation that query and table cannot be specified at the same time for `SpannerIO.read()`. Previously `withQuery` overrides `withTable`, if set ([#24956](https://github.com/apache/beam/issues/24956)).

## Bug fixes

* [BigQueryIO] Fixed a bug in batch Storage Write API that frequently exhausted concurrent connections quota ([#31710](https://github.com/apache/beam/pull/31710))

## Known Issues

* Python pipelines that run with 2.53.0-2.58.0 SDKs and read data from GCS might be affected by a data corruption issue ([#32169](https://github.com/apache/beam/issues/32169)). The issue will be fixed in 2.59.0 ([#32135](https://github.com/apache/beam/pull/32135)). To work around this, update the google-cloud-storage package to version 2.18.2 or newer.
* [KafkaIO] Records read with `ReadFromKafkaViaSDF` are redistributed and may contain duplicates regardless of the configuration. This affects Java pipelines with Dataflow v2 runner and xlang pipelines reading from Kafka, ([#32196](https://github.com/apache/beam/issues/32196))
* BigQuery Enrichment (Python):  The following issues are present when using the BigQuery enrichment transform ([#32780](https://github.com/apache/beam/pull/32780)):
  * Duplicate Rows: Multiple conditions may be applied incorrectly, leading to the duplication of rows in the output.
  * Incorrect Results with Batched Requests: Conditions may not be correctly scoped to individual rows within the batch, potentially causing inaccurate results.
  * Fixed in 2.61.0.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.58.0 release. Thank you to all contributors!

Ahmed Abualsaud

Ahmet Altay

Alexandre Moueddene

Alexey Romanenko

Andrew Crites

Bartosz Zablocki

Celeste Zeng

Chamikara Jayalath

Clay Johnson

Damon Douglass

Danny McCormick

Dilnaz Amanzholova

Florian Bernard

Francis O'Hara

George Ma

Israel Herraiz

Jack McCluskey

Jaehyeon Kim

James Roseman

Kenneth Knowles

Maciej Szwaja

Michel Davit

Minh Son Nguyen

Naireen

Niel Markwick

Oliver Cardoza

Robert Bradshaw

Robert Burke

Rohit Sinha

S. Veyrié

Sam Whittle

Shunping Huang

Svetak Sundhar

TongruiLi

Tony Tang

Valentyn Tymofieiev

Vitaly Terentyev

Yi Hu