---
title:  "Apache Beam 2.46.0"
date:   2023-03-10 13:00:00 -0500
categories:
  - blog
  - release
authors:
  - damccorm
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

We are happy to present the new 2.46.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2460-2023-03-10) for this release.

<!--more-->

For more information on changes in 2.46.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/9?closed=1).

## Highlights

* Java SDK containers migrated to [Eclipse Temurin](https://hub.docker.com/_/eclipse-temurin)
  as a base. This change migrates away from the deprecated [OpenJDK](https://hub.docker.com/_/openjdk)
  container. Eclipse Temurin is currently based upon Ubuntu 22.04 while the OpenJDK
  container was based upon Debian 11.
* RunInference PTransform will accept model paths as SideInputs in Python SDK. ([#24042](https://github.com/apache/beam/issues/24042))
* RunInference supports ONNX runtime in Python SDK ([#22972](https://github.com/apache/beam/issues/22972))
* Tensorflow Model Handler for RunInference in Python SDK ([#25366](https://github.com/apache/beam/issues/25366))
* Java SDK modules migrated to use `:sdks:java:extensions:avro` ([#24748](https://github.com/apache/beam/issues/24748))

## I/Os

* Added in JmsIO a retry policy for failed publications (Java) ([#24971](https://github.com/apache/beam/issues/24971)).
* Support for `LZMA` compression/decompression of text files added to the Python SDK ([#25316](https://github.com/apache/beam/issues/25316))
* Added ReadFrom/WriteTo Csv/Json as top-level transforms to the Python SDK.

## New Features / Improvements

* Add UDF metrics support for Samza portable mode.
* Option for SparkRunner to avoid the need of SDF output to fit in memory ([#23852](https://github.com/apache/beam/issues/23852)).
  This helps e.g. with ParquetIO reads. Turn the feature on by adding experiment `use_bounded_concurrent_output_for_sdf`.
* Add `WatchFilePattern` transform, which can be used as a side input to the RunInference PTransfrom to watch for model updates using a file pattern. ([#24042](https://github.com/apache/beam/issues/24042))
* Add support for loading TorchScript models with `PytorchModelHandler`. The TorchScript model path can be
  passed to PytorchModelHandler using `torch_script_model_path=<path_to_model>`. ([#25321](https://github.com/apache/beam/pull/25321))
* The Go SDK now requires Go 1.19 to build. ([#25545](https://github.com/apache/beam/pull/25545))
* The Go SDK now has an initial native Go implementation of a portable Beam Runner called Prism. ([#24789](https://github.com/apache/beam/pull/24789))
  * For more details and current state see https://github.com/apache/beam/tree/master/sdks/go/pkg/beam/runners/prism.

## Breaking Changes

* The deprecated SparkRunner for Spark 2 (see [2.41.0](#2410---2022-08-23)) was removed ([#25263](https://github.com/apache/beam/pull/25263)).
* Python's BatchElements performs more aggressive batching in some cases,
  capping at 10 second rather than 1 second batches by default and excluding
  fixed cost in this computation to better handle cases where the fixed cost
  is larger than a single second. To get the old behavior, one can pass
  `target_batch_duration_secs_including_fixed_cost=1` to BatchElements.

## Deprecations

* Avro related classes are deprecated in module `beam-sdks-java-core` and will be eventually removed. Please, migrate to a new module `beam-sdks-java-extensions-avro` instead by importing the classes from `org.apache.beam.sdk.extensions.avro` package.
  For the sake of migration simplicity, the relative package path and the whole class hierarchy of Avro related classes in new module is preserved the same as it was before.
  For example, import `org.apache.beam.sdk.extensions.avro.coders.AvroCoder` class instead of`org.apache.beam.sdk.coders.AvroCoder`. ([#24749](https://github.com/apache/beam/issues/24749)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.46.0 release. Thank you to all contributors!

Ahmet Altay

Alan Zhang

Alexey Romanenko

Amrane Ait Zeouay

Anand Inguva

Andrew Pilloud

Brian Hulette

Bruno Volpato

Byron Ellis

Chamikara Jayalath

Damon

Danny McCormick

Darkhan Nausharipov

David Katz

Dmitry Repin

Doug Judd

Egbert van der Wal

Elizaveta Lomteva

Evan Galpin

Herman Mak

Jack McCluskey

Jan Lukavský

Johanna Öjeling

John Casey

Jozef Vilcek

Junhao Liu

Juta Staes

Katie Liu

Kiley Sok

Liam Miller-Cushon

Luke Cwik

Moritz Mack

Ning Kang

Oleh Borysevych

Pablo E

Pablo Estrada

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Ruslan Altynnikov

Ryan Zhang

Sam Rohde

Sam Whittle

Sam sam

Sergei Lilichenko

Shivam

Shubham Krishna

Theodore Ni

Timur Sultanov

Tony Tang

Vachan

Veronica Wasson

Vincent Devillers

Vitaly Terentyev

William Ross Morrow

Xinyu Liu

Yi Hu

ZhengLin Li

Ziqi Ma

ahmedabu98

alexeyinkin

aliftadvantage

bullet03

dannikay

darshan-sj

dependabot[bot]

johnjcasey

kamrankoupayi

kileys

liferoad

nancyxu123

nickuncaged1201

pablo rodriguez defino

tvalentyn

xqhu
