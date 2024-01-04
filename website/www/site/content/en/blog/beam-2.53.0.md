---
title:  "Apache Beam 2.53.0"
date:   2024-01-04 09:00:00 -0400
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

We are happy to present the new 2.53.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/) for this release.

<!--more-->

For more information on changes in 2.53.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/17).

## Highlights

* Python streaming users that use 2.47.0 and newer versions of Beam should update to version 2.53.0, which fixes a known issue: ([#27330](https://github.com/apache/beam/issues/27330)).

## I/Os

* TextIO now supports skipping multiple header lines (Java) ([#17990](https://github.com/apache/beam/issues/17990)).
* Python GCSIO is now implemented with GCP GCS Client instead of apitools ([#25676](https://github.com/apache/beam/issues/25676))
* Adding support for LowCardinality DataType in ClickHouse (Java) ([#29533](https://github.com/apache/beam/pull/29533)).
* Added support for handling bad records to KafkaIO (Java) ([#29546](https://github.com/apache/beam/pull/29546))
* Add support for generating text embeddings in MLTransform for Vertex AI and Hugging Face Hub models.([#29564](https://github.com/apache/beam/pull/29564))
* NATS IO connector added (Go) ([#29000](https://github.com/apache/beam/issues/29000)).

## New Features / Improvements

* The Python SDK now type checks `collections.abc.Collections` types properly. Some type hints that were erroneously allowed by the SDK may now fail. ([#29272](https://github.com/apache/beam/pull/29272))
* Running multi-language pipelines locally no longer requires Docker.
  Instead, the same (generally auto-started) subprocess used to perform the
  expansion can also be used as the cross-language worker.
* Framework for adding Error Handlers to composite transforms added in Java ([#29164](https://github.com/apache/beam/pull/29164)).
* Python 3.11 images now include google-cloud-profiler ([#29561](https://github.com/apache/beam/pull/29651)).

## Breaking Changes

* Upgraded to go 1.21.5 to build, fixing [CVE-2023-45285](https://security-tracker.debian.org/tracker/CVE-2023-45285) and [CVE-2023-39326](https://security-tracker.debian.org/tracker/CVE-2023-39326)

## Deprecations

* Euphoria DSL is deprecated and will be removed in a future release (not before 2.56.0) ([#29451](https://github.com/apache/beam/issues/29451))

## Bugfixes

* (Python) Fixed sporadic crashes in streaming pipelines that affected some users of 2.47.0 and newer SDKs ([#27330](https://github.com/apache/beam/issues/27330)).
* (Python) Fixed a bug that caused MLTransform to drop identical elements in the output PCollection ([#29600](https://github.com/apache/beam/issues/29600)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.53.0 release. Thank you to all contributors!

Ahmed Abualsaud

Ahmet Altay

Alexey Romanenko

Anand Inguva

Arun Pandian

Balázs Németh

Bruno Volpato

Byron Ellis

Calvin Swenson Jr

Chamikara Jayalath

Clay Johnson

Damon

Danny McCormick

Ferran Fernández Garrido

Georgii Zemlianyi

Israel Herraiz

Jack McCluskey

Jacob Tomlinson

Jan Lukavský

JayajP

Jeffrey Kinard

Johanna Öjeling

Julian Braha

Julien Tournay

Kenneth Knowles

Lawrence Qiu

Mark Zitnik

Mattie Fu

Michel Davit

Mike Williamson

Naireen

Naireen Hussain

Niel Markwick

Pablo Estrada

Radosław Stankiewicz

Rebecca Szper

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Sam Rohde

Sam Whittle

Shunping Huang

Svetak Sundhar

Talat UYARER

Tom Stepp

Tony Tang

Vlado Djerek

Yi Hu

Zechen Jiang

clmccart

damccorm

darshan-sj

gabry.wu

johnjcasey

liferoad

lrakla

martin trieu

tvalentyn