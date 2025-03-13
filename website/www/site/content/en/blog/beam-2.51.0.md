---
title:  "Apache Beam 2.51.0"
date:   2023-10-11 09:00:00 -0400
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

We are happy to present the new 2.51.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2510-2023-10-03) for this release.

<!--more-->

For more information on changes in 2.51.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/15).

## New Features / Improvements

* In Python, [RunInference](https://beam.apache.org/documentation/sdks/python-machine-learning/#why-use-the-runinference-api) now supports loading many models in the same transform using a [KeyedModelHandler](https://beam.apache.org/documentation/sdks/python-machine-learning/#use-a-keyed-modelhandler) ([#27628](https://github.com/apache/beam/issues/27628)).
* In Python, the [VertexAIModelHandlerJSON](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.vertex_ai_inference.html#apache_beam.ml.inference.vertex_ai_inference.VertexAIModelHandlerJSON) now supports passing in inference_args. These will be passed through to the Vertex endpoint as parameters.
* Added support to run `mypy` on user pipelines ([#27906](https://github.com/apache/beam/issues/27906))


## Breaking Changes

* Removed fastjson library dependency for Beam SQL. Table property is changed to be based on jackson ObjectNode (Java) ([#24154](https://github.com/apache/beam/issues/24154)).
* Removed TensorFlow from Beam Python container images [PR](https://github.com/apache/beam/pull/28424). If you have been negatively affected by this change, please comment on [#20605](https://github.com/apache/beam/issues/20605).
* Removed the parameter `t reflect.Type` from `parquetio.Write`. The element type is derived from the input PCollection (Go) ([#28490](https://github.com/apache/beam/issues/28490))
* Refactor BeamSqlSeekableTable.setUp adding a parameter joinSubsetType. [#28283](https://github.com/apache/beam/issues/28283)


## Bugfixes

* Fixed exception chaining issue in GCS connector (Python) ([#26769](https://github.com/apache/beam/issues/26769#issuecomment-1700422615)).
* Fixed streaming inserts exception handling, GoogleAPICallErrors are now retried according to retry strategy and routed to failed rows where appropriate rather than causing a pipeline error (Python) ([#21080](https://github.com/apache/beam/issues/21080)).
* Fixed a bug in Python SDK's cross-language Bigtable sink that mishandled records that don't have an explicit timestamp set: [#28632](https://github.com/apache/beam/issues/28632).


## Security Fixes
* Python containers updated, fixing [CVE-2021-30474](https://nvd.nist.gov/vuln/detail/CVE-2021-30474), [CVE-2021-30475](https://nvd.nist.gov/vuln/detail/CVE-2021-30475), [CVE-2021-30473](https://nvd.nist.gov/vuln/detail/CVE-2021-30473), [CVE-2020-36133](https://nvd.nist.gov/vuln/detail/CVE-2020-36133), [CVE-2020-36131](https://nvd.nist.gov/vuln/detail/CVE-2020-36131), [CVE-2020-36130](https://nvd.nist.gov/vuln/detail/CVE-2020-36130), and [CVE-2020-36135](https://nvd.nist.gov/vuln/detail/CVE-2020-36135)
* Used go 1.21.1 to build, fixing [CVE-2023-39320](https://security-tracker.debian.org/tracker/CVE-2023-39320)


## Known Issues

* Python pipelines using BigQuery Storage Read API must pin `fastavro` dependency to 1.8.3
  or earlier: [#28811](https://github.com/apache/beam/issues/28811)

## List of Contributors

According to git shortlog, the following people contributed to the 2.50.0 release. Thank you to all contributors!

Adam Whitmore

Ahmed Abualsaud

Ahmet Altay

Aleksandr Dudko

Alexey Romanenko

Anand Inguva

Andrey Devyatkin

Arvind Ram

Arwin Tio

BjornPrime

Bruno Volpato

Bulat

Celeste Zeng

Chamikara Jayalath

Clay Johnson

Damon

Danny McCormick

David Cavazos

Dip Patel

Hai Joey Tran

Hao Xu

Haruka Abe

Jack Dingilian

Jack McCluskey

Jeff Kinard

Jeffrey Kinard

Joey Tran

Johanna Öjeling

Julien Tournay

Kenneth Knowles

Kerry Donny-Clark

Mattie Fu

Melissa Pashniak

Michel Davit

Moritz Mack

Pranav Bhandari

Rebecca Szper

Reeba Qureshi

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Ruwann

Ryan Tam

Sam Rohde

Sereana Seim

Svetak Sundhar

Tim Grein

Udi Meiri

Valentyn Tymofieiev

Vitaly Terentyev

Vlado Djerek

Xinyu Liu

Yi Hu

Zbynek Konecny

Zechen Jiang

bzablocki

caneff

dependabot[bot]

gDuperran

gabry.wu

johnjcasey

kberezin-nshl

kennknowles

liferoad

lostluck

magicgoody

martin trieu

mosche

olalamichelle

tvalentyn

xqhu

Łukasz Spyra
