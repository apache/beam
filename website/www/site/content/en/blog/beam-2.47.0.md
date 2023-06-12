---
title:  "Apache Beam 2.47.0"
date:   2023-05-10 12:00:00 -0500
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

We are happy to present the new 2.47.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2470-2023-05-10) for this release.

<!--more-->

For more information on changes in 2.47.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/10).

## Highlights

* Apache Beam adds Python 3.11 support ([#23848](https://github.com/apache/beam/issues/23848)).

## I/Os

* BigQuery Storage Write API is now available in Python SDK via cross-language ([#21961](https://github.com/apache/beam/issues/21961)).
* Added HbaseIO support for writing RowMutations (ordered by rowkey) to Hbase (Java) ([#25830](https://github.com/apache/beam/issues/25830)).
* Added fileio transforms MatchFiles, MatchAll and ReadMatches (Go) ([#25779](https://github.com/apache/beam/issues/25779)).
* Add integration test for JmsIO + fix issue with multiple connections (Java) ([#25887](https://github.com/apache/beam/issues/25887)).

## New Features / Improvements

* The Flink runner now supports Flink 1.16.x ([#25046](https://github.com/apache/beam/issues/25046)).
* Schema'd PTransforms can now be directly applied to Beam dataframes just like PCollections.
  (Note that when doing multiple operations, it may be more efficient to explicitly chain the operations
  like `df | (Transform1 | Transform2 | ...)` to avoid excessive conversions.)
* The Go SDK adds new transforms periodic.Impulse and periodic.Sequence that extends support
  for slowly updating side input patterns. ([#23106](https://github.com/apache/beam/issues/23106))
* Several Google client libraries in Python SDK dependency chain were updated to latest available major versions. ([#24599](https://github.com/apache/beam/pull/24599))

## Breaking Changes

* If a main session fails to load, the pipeline will now fail at worker startup. ([#25401](https://github.com/apache/beam/issues/25401)).
* Python pipeline options will now ignore unparsed command line flags prefixed with a single dash. ([#25943](https://github.com/apache/beam/issues/25943)).
* The SmallestPerKey combiner now requires keyword-only arguments for specifying optional parameters, such as `key` and `reverse`. ([#25888](https://github.com/apache/beam/issues/25888)).

## Deprecations

* Cloud Debugger support and its pipeline options are deprecated and will be removed in the next Beam version,
  in response to the Google Cloud Debugger service [turning down](https://cloud.google.com/debugger/docs/deprecations). (Java) ([#25959](https://github.com/apache/beam/issues/25959)).

## Bugfixes

* BigQuery sink in STORAGE_WRITE_API mode in batch pipelines might result in data consistency issues during the handling of other unrelated transient errors for Beam SDKs 2.35.0 - 2.46.0 (inclusive). For more details see: https://github.com/apache/beam/issues/26521

### Known Issues

* BigQueryIO Storage API write with autoUpdateSchema may cause data corruption for Beam SDKs 2.45.0 - 2.47.0 (inclusive) ([#26789](https://github.com/apache/beam/issues/26789))

## List of Contributors

According to git shortlog, the following people contributed to the 2.47.0 release. Thank you to all contributors!

Ahmed Abualsaud

Ahmet Altay

Alexey Romanenko

Amir Fayazi

Amrane Ait Zeouay

Anand Inguva

Andrew Pilloud

Andrey Kot

Bjorn Pedersen

Bruno Volpato

Buqian Zheng

Chamikara Jayalath

ChangyuLi28

Damon

Danny McCormick

Dmitry Repin

George Ma

Jack Dingilian

Jack McCluskey

Jasper Van den Bossche

Jeremy Edwards

Jiangjie (Becket) Qin

Johanna Öjeling

Juta Staes

Kenneth Knowles

Kyle Weaver

Mattie Fu

Moritz Mack

Nick Li

Oleh Borysevych

Pablo Estrada

Rebecca Szper

Reuven Lax

Reza Rokni

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Saadat Su

Saifuddin53

Sam Rohde

Shubham Krishna

Svetak Sundhar

Theodore Ni

Thomas Gaddy

Timur Sultanov

Udi Meiri

Valentyn Tymofieiev

Xinyu Liu

Yanan Hao

Yi Hu

Yuvi Panda

andres-vv

bochap

dannikay

darshan-sj

dependabot[bot]

harrisonlimh

hnnsgstfssn

jrmccluskey

liferoad

tvalentyn

xianhualiu

zhangskz