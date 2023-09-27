---
title:  "Apache Beam 2.50.0"
date:   2023-08-30 09:00:00 -0400
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

We are happy to present the new 2.50.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2500-2023-08-30) for this release.

<!--more-->

For more information on changes in 2.50.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/14).

## Highlights

* Spark 3.2.2 is used as default version for Spark runner ([#23804](https://github.com/apache/beam/issues/23804)).
* The Go SDK has a new default local runner, called Prism ([#24789](https://github.com/apache/beam/issues/24789)).
* All Beam released container images are now [multi-arch images](https://cloud.google.com/kubernetes-engine/docs/how-to/build-multi-arch-for-arm#what_is_a_multi-arch_image) that support both x86 and ARM CPU architectures.

## I/Os

* Java KafkaIO now supports picking up topics via topicPattern ([#26948](https://github.com/apache/beam/pull/26948))
* Support for read from Cosmos DB Core SQL API ([#23604](https://github.com/apache/beam/issues/23604))
* Upgraded to HBase 2.5.5 for HBaseIO. (Java) ([#27711](https://github.com/apache/beam/issues/19554))
* Added support for GoogleAdsIO source (Java) ([#27681](https://github.com/apache/beam/pull/27681)).

## New Features / Improvements

* The Go SDK now requires Go 1.20 to build. ([#27558](https://github.com/apache/beam/issues/27558))
* The Go SDK has a new default local runner, Prism. ([#24789](https://github.com/apache/beam/issues/24789)).
  * Prism is a portable runner that executes each transform independantly, ensuring coders.
  * At this point it supercedes the Go direct runner in functionality. The Go direct runner is now deprecated.
  * See https://github.com/apache/beam/blob/master/sdks/go/pkg/beam/runners/prism/README.md for the goals and features of Prism.
* Hugging Face Model Handler for RunInference added to Python SDK. ([#26632](https://github.com/apache/beam/pull/26632))
* Hugging Face Pipelines support for RunInference added to Python SDK. ([#27399](https://github.com/apache/beam/pull/27399))
* Vertex AI Model Handler for RunInference now supports private endpoints ([#27696](https://github.com/apache/beam/pull/27696))
* MLTransform transform added with support for common ML pre/postprocessing operations ([#26795](https://github.com/apache/beam/pull/26795))
* Upgraded the Kryo extension for the Java SDK to Kryo 5.5.0. This brings in bug fixes, performance improvements, and serialization of Java 14 records. ([#27635](https://github.com/apache/beam/issues/27635))
* All Beam released container images are now [multi-arch images](https://cloud.google.com/kubernetes-engine/docs/how-to/build-multi-arch-for-arm#what_is_a_multi-arch_image) that support both x86 and ARM CPU architectures. ([#27674](https://github.com/apache/beam/issues/27674)). The multi-arch container images include:
  * All versions of Go, Python, Java and Typescript SDK containers.
  * All versions of Flink job server containers.
  * Java and Python expansion service containers.
  * Transform service controller container.
  * Spark3 job server container.
* Added support for batched writes to AWS SQS for improved throughput (Java, AWS 2).([#21429](https://github.com/apache/beam/issues/21429))

## Breaking Changes

* Python SDK: Legacy runner support removed from Dataflow, all pipelines must use runner v2.
* Python SDK: Dataflow Runner will no longer stage Beam SDK from PyPI in the `--staging_location` at pipeline submission. Custom container images that are not based on Beam's default image must include Apache Beam installation.([#26996](https://github.com/apache/beam/issues/26996))

## Deprecations

* The Go Direct Runner is now Deprecated. It remains available to reduce migration churn.
  * Tests can be set back to the direct runner by overriding TestMain: `func TestMain(m *testing.M) { ptest.MainWithDefault(m, "direct") }`
  * It's recommended to fix issues seen in tests using Prism, as they can also happen on any portable runner.
  * Use the generic register package for your pipeline DoFns to ensure pipelines function on portable runners, like prism.
  * Do not rely on closures or using package globals for DoFn configuration. They don't function on portable runners.

## Bugfixes

* Fixed DirectRunner bug in Python SDK where GroupByKey gets empty PCollection and fails when pipeline option `direct_num_workers!=1`.([#27373](https://github.com/apache/beam/pull/27373))
* Fixed BigQuery I/O bug when estimating size on queries that utilize row-level security ([#27474](https://github.com/apache/beam/pull/27474))
* Beam Python containers rely on a version of Debian/aom that has several security vulnerabilities: [CVE-2021-30474](https://nvd.nist.gov/vuln/detail/CVE-2021-30474), [CVE-2021-30475](https://nvd.nist.gov/vuln/detail/CVE-2021-30475), [CVE-2021-30473](https://nvd.nist.gov/vuln/detail/CVE-2021-30473), [CVE-2020-36133](https://nvd.nist.gov/vuln/detail/CVE-2020-36133), [CVE-2020-36131](https://nvd.nist.gov/vuln/detail/CVE-2020-36131), [CVE-2020-36130](https://nvd.nist.gov/vuln/detail/CVE-2020-36130), and [CVE-2020-36135](https://nvd.nist.gov/vuln/detail/CVE-2020-36135).

## Known Issues

* Long-running Python pipelines might experience a memory leak: [#28246](https://github.com/apache/beam/issues/28246).
* Python Pipelines using BigQuery IO or `orjson` dependency might experience segmentation faults or get stuck: [#28318](https://github.com/apache/beam/issues/28318).
* Python SDK's cross-language Bigtable sink mishandles records that don't have an explicit timestamp set: [#28632](https://github.com/apache/beam/issues/28632). To avoid this issue, set explicit timestamps for all records before writing to Bigtable.


## List of Contributors

According to git shortlog, the following people contributed to the 2.50.0 release. Thank you to all contributors!

Abacn

acejune

AdalbertMemSQL

ahmedabu98

Ahmed Abualsaud

al97

Aleksandr Dudko

Alexey Romanenko

Anand Inguva

Andrey Devyatkin

Anton Shalkovich

ArjunGHUB

Bjorn Pedersen

BjornPrime

Brett Morgan

Bruno Volpato

Buqian Zheng

Burke Davison

Byron Ellis

bzablocki

case-k

Celeste Zeng

Chamikara Jayalath

Clay Johnson

Connor Brett

Damon

Damon Douglas

Dan Hansen

Danny McCormick

Darkhan Nausharipov

Dip Patel

Dmytro Sadovnychyi

Florent Biville

Gabriel Lacroix

Hai Joey Tran

Hong Liang Teoh

Jack McCluskey

James Fricker

Jeff Kinard

Jeff Zhang

Jing

johnjcasey

jon esperanza

Josef Šimánek

Kenneth Knowles

Laksh

Liam Miller-Cushon

liferoad

magicgoody

Mahmud Ridwan

Manav Garg

Marco Vela

martin trieu

Mattie Fu

Michel Davit

Moritz Mack

mosche

Peter Sobot

Pranav Bhandari

Reeba Qureshi

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

RyuSA

Saba Sathya

Sam Whittle

Steven Niemitz

Steven van Rossum

Svetak Sundhar

Tony Tang

Valentyn Tymofieiev

Vitaly Terentyev

Vlado Djerek

Yichi Zhang

Yi Hu

Zechen Jiang

