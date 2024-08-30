---
title:  "Apache Beam 2.49.0"
date:   2023-07-17 09:00:00 -0400
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

We are happy to present the new 2.49.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2490-2023-07-17) for this release.

<!--more-->

For more information on changes in 2.49.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/13).

## I/Os

* Support for Bigtable Change Streams added in Java `BigtableIO.ReadChangeStream` ([#27183](https://github.com/apache/beam/issues/27183)).
* Added Bigtable Read and Write cross-language transforms to Python SDK (([#26593](https://github.com/apache/beam/issues/26593)), ([#27146](https://github.com/apache/beam/issues/27146))).

## New Features / Improvements

* Allow prebuilding large images when using `--prebuild_sdk_container_engine=cloud_build`, like images depending on `tensorflow` or `torch` ([#27023](https://github.com/apache/beam/pull/27023)).
* Disabled `pip` cache when installing packages on the workers. This reduces the size of prebuilt Python container images ([#27035](https://github.com/apache/beam/pull/27035)).
* Select dedicated avro datum reader and writer (Java) ([#18874](https://github.com/apache/beam/issues/18874)).
* Timer API for the Go SDK (Go) ([#22737](https://github.com/apache/beam/issues/22737)).


## Deprecations

* Remove Python 3.7 support. ([#26447](https://github.com/apache/beam/issues/26447))

## Bugfixes

* Fixed KinesisIO `NullPointerException` when a progress check is made before the reader is started (IO) ([#23868](https://github.com/apache/beam/issues/23868))

### Known Issues

* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).
* Python SDK's cross-language Bigtable sink mishandles records that don't have an explicit timestamp set: [#28632](https://github.com/apache/beam/issues/28632). To avoid this issue, set explicit timestamps for all records before writing to Bigtable.
* Python pipelines using the `--impersonate_service_account` option with BigQuery IOs might fail on Dataflow ([#32030](https://github.com/apache/beam/issues/32030)). This is fixed in 2.59.0 release.


## List of Contributors

According to git shortlog, the following people contributed to the 2.49.0 release. Thank you to all contributors!

Abzal Tuganbay

AdalbertMemSQL

Ahmed Abualsaud

Ahmet Altay

Alan Zhang

Alexey Romanenko

Anand Inguva

Andrei Gurau

Arwin Tio

Bartosz Zablocki

Bruno Volpato

Burke Davison

Byron Ellis

Chamikara Jayalath

Charles Rothrock

Chris Gavin

Claire McGinty

Clay Johnson

Damon

Daniel Dopierała

Danny McCormick

Darkhan Nausharipov

David Cavazos

Dip Patel

Dmitry Repin

Gavin McDonald

Jack Dingilian

Jack McCluskey

James Fricker

Jan Lukavský

Jasper Van den Bossche

John Casey

John Gill

Joseph Crowley

Kanishk Karanawat

Katie Liu

Kenneth Knowles

Kyle Galloway

Liam Miller-Cushon

MakarkinSAkvelon

Masato Nakamura

Mattie Fu

Michel Davit

Naireen Hussain

Nathaniel Young

Nelson Osacky

Nick Li

Oleh Borysevych

Pablo Estrada

Reeba Qureshi

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Rouslan

Saadat Su

Sam Rohde

Sam Whittle

Sanil Jain

Shunping Huang

Smeet nagda

Svetak Sundhar

Timur Sultanov

Udi Meiri

Valentyn Tymofieiev

Vlado Djerek

WuA

XQ Hu

Xianhua Liu

Xinyu Liu

Yi Hu

Zachary Houfek

alexeyinkin

bigduu

bullet03

bzablocki

jonathan-lemos

jubebo

magicgoody

ruslan-ikhsan

sultanalieva-s

vitaly.terentyev

