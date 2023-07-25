---
title:  "Apache Beam 2.48.0"
date:   2023-05-31 11:30:00 -0400
categories:
  - blog
  - release
authors:
  - riteshghorse
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

We are happy to present the new 2.48.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2480-2023-05-31) for this release.

<!--more-->

For more information on changes in 2.48.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/12).

**Note: The release tag for Go SDK for this release is sdks/v2.48.2 instead of sdks/v2.48.0 because of incorrect commit attached to the release tag sdks/v2.48.0.**

## Highlights

* "Experimental" annotation cleanup: the annotation and concept have been removed from Beam to avoid
  the misperception of code as "not ready". Any proposed breaking changes will be subject to
  case-by-case pro/con decision making (and generally avoided) rather than using the "Experimental"
  to allow them.

## I/Os

* Added rename for GCS and copy for local filesystem (Go) ([#25779](https://github.com/apache/beam/issues/26064)).
* Added support for enhanced fan-out in KinesisIO.Read (Java) ([#19967](https://github.com/apache/beam/issues/19967)).
  * This change is not compatible with Flink savepoints created by Beam 2.46.0 applications which had KinesisIO sources.
* Added textio.ReadWithFilename transform (Go) ([#25812](https://github.com/apache/beam/issues/25812)).
* Added fileio.MatchContinuously transform (Go) ([#26186](https://github.com/apache/beam/issues/26186)).

## New Features / Improvements

* Allow passing service name for google-cloud-profiler (Python) ([#26280](https://github.com/apache/beam/issues/26280)).
* Dead letter queue support added to RunInference in Python ([#24209](https://github.com/apache/beam/issues/24209)).
* Support added for defining pre/postprocessing operations on the RunInference transform ([#26308](https://github.com/apache/beam/issues/26308))
* Adds a Docker Compose based transform service that can be used to discover and use portable Beam transforms ([#26023](https://github.com/apache/beam/pull/26023)).

## Breaking Changes

* Passing a tag into MultiProcessShared is now required in the Python SDK ([#26168](https://github.com/apache/beam/issues/26168)).
* CloudDebuggerOptions is removed (deprecated in Beam v2.47.0) for Dataflow runner as the Google Cloud Debugger service is [shutting down](https://cloud.google.com/debugger/docs/deprecations). (Java) ([#25959](https://github.com/apache/beam/issues/25959)).
* AWS 2 client providers (deprecated in Beam [v2.38.0](#2380---2022-04-20)) are finally removed ([#26681](https://github.com/apache/beam/issues/26681)).
* AWS 2 SnsIO.writeAsync (deprecated in Beam v2.37.0 due to risk of data loss) was finally removed ([#26710](https://github.com/apache/beam/issues/26710)).
* AWS 2 coders (deprecated in Beam v2.43.0 when adding Schema support for AWS Sdk Pojos) are finally removed ([#23315](https://github.com/apache/beam/issues/23315)).

## Bugfixes

* Fixed Java bootloader failing with Too Long Args due to long classpaths, with a pathing jar. (Java) ([#25582](https://github.com/apache/beam/issues/25582)).

## Known Issues

* PubsubIO writes will throw *SizeLimitExceededException* for any message above 100 bytes, when used in batch (bounded) mode. (Java) ([#27000](https://github.com/apache/beam/issues/27000)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.48.0 release. Thank you to all contributors!

Abzal Tuganbay

Ahmed Abualsaud

Alexey Romanenko

Anand Inguva

Andrei Gurau

Andrey Devyatkin

Balázs Németh

Bazyli Polednia

Bruno Volpato

Chamikara Jayalath

Clay Johnson

Damon

Daniel Arn

Danny McCormick

Darkhan Nausharipov

Dip Patel

Dmitry Repin

George Novitskiy

Israel Herraiz

Jack Dingilian

Jack McCluskey

Jan Lukavský

Jasper Van den Bossche

Jeff Zhang

Jeremy Edwards

Johanna Öjeling

John Casey

Katie Liu

Kenneth Knowles

Kerry Donny-Clark

Kuba Rauch

Liam Miller-Cushon

MakarkinSAkvelon

Mattie Fu

Michel Davit

Moritz Mack

Nick Li

Oleh Borysevych

Pablo Estrada

Pranav Bhandari

Pranjal Joshi

Rebecca Szper

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Rouslan

RuiLong J

RyujiTamaki

Sam Whittle

Sanil Jain

Svetak Sundhar

Timur Sultanov

Tony Tang

Udi Meiri

Valentyn Tymofieiev

Vishal Bhise

Vitaly Terentyev

Xinyu Liu

Yi Hu

bullet03

darshan-sj

kellen

liferoad

mokamoka03210120

psolomin