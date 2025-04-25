---
title:  "Apache Beam 2.52.0"
date:   2023-11-17 09:00:00 -0400
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

We are happy to present the new 2.52.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2520-2023-11-17) for this release.

<!--more-->

For more information on changes in 2.52.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/16).

## Highlights

* Previously deprecated Avro-dependent code (Beam Release 2.46.0) has been finally removed from Java SDK "core" package.
Please, use `beam-sdks-java-extensions-avro` instead. This will allow to easily update Avro version in user code without
potential breaking changes in Beam "core" since the Beam Avro extension already supports the latest Avro versions and
should handle this. ([#25252](https://github.com/apache/beam/issues/25252)).
* Publishing Java 21 SDK container images now supported as part of Apache Beam release process. ([#28120](https://github.com/apache/beam/issues/28120))
  * Direct Runner and Dataflow Runner support running pipelines on Java21 (experimental until tests fully setup). For other runners (Flink, Spark, Samza, etc) support status depend on runner projects.

## New Features / Improvements

* Add `UseDataStreamForBatch` pipeline option to the Flink runner. When it is set to true, Flink runner will run batch
  jobs using the DataStream API. By default the option is set to false, so the batch jobs are still executed
  using the DataSet API.
* `upload_graph` as one of the Experiments options for DataflowRunner is no longer required when the graph is larger than 10MB for Java SDK ([PR#28621](https://github.com/apache/beam/pull/28621)).
* state amd side input cache has been enabled to a default of 100 MB. Use `--max_cache_memory_usage_mb=X` to provide cache size for the user state API and side inputs. (Python) ([#28770](https://github.com/apache/beam/issues/28770)).
* Beam YAML stable release. Beam pipelines can now be written using YAML and leverage the Beam YAML framework which includes a preliminary set of IO's and turnkey transforms. More information can be found in the YAML root folder and in the [README](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/README.md).

## Breaking Changes

* `org.apache.beam.sdk.io.CountingSource.CounterMark` uses custom `CounterMarkCoder` as a default coder since all Avro-dependent
classes finally moved to `extensions/avro`. In case if it's still required to use `AvroCoder` for `CounterMark`, then,
as a workaround, a copy of "old" `CountingSource` class should be placed into a project code and used directly
([#25252](https://github.com/apache/beam/issues/25252)).
* Renamed `host` to `firestoreHost` in `FirestoreOptions` to avoid potential conflict of command line arguments (Java) ([#29201](https://github.com/apache/beam/pull/29201)).
* Transforms which use `SnappyCoder` are update incompatible with previous versions of the same transform (Java) on some runners. This includes PubSubIO's read ([#28655](https://github.com/apache/beam/pull/28655#issuecomment-2407839769)).

## Bugfixes

* Fixed "Desired bundle size 0 bytes must be greater than 0" in Java SDK's BigtableIO.BigtableSource when you have more cores than bytes to read (Java) [#28793](https://github.com/apache/beam/issues/28793).
* `watch_file_pattern` arg of the [RunInference](https://github.com/apache/beam/blob/104c10b3ee536a9a3ea52b4dbf62d86b669da5d9/sdks/python/apache_beam/ml/inference/base.py#L997) arg had no effect prior to 2.52.0. To use the behavior of arg `watch_file_pattern` prior to 2.52.0, follow the documentation at https://beam.apache.org/documentation/ml/side-input-updates/ and use `WatchFilePattern` PTransform as a SideInput. ([#28948](https://github.com/apache/beam/pulls/28948))
* `MLTransform` doesn't output artifacts such as min, max and quantiles. Instead, `MLTransform` will add a feature to output these artifacts as human readable format - [#29017](https://github.com/apache/beam/issues/29017). For now, to use the artifacts such as min and max that were produced by the eariler `MLTransform`, use `read_artifact_location` of `MLTransform`, which reads artifacts that were produced earlier in a different `MLTransform` ([#29016](https://github.com/apache/beam/pull/29016/))
* Fixed a memory leak, which affected some long-running Python pipelines: [#28246](https://github.com/apache/beam/issues/28246).

## Security Fixes
* Fixed [CVE-2023-39325](https://www.cve.org/CVERecord?id=CVE-2023-39325) (Java/Python/Go) ([#29118](https://github.com/apache/beam/issues/29118)).
* Mitigated [CVE-2023-47248](https://nvd.nist.gov/vuln/detail/CVE-2023-47248)  (Python) [#29392](https://github.com/apache/beam/issues/29392).

## Known issues
* MLTransform drops the identical elements in the output PCollection. For any duplicate elements, a single element will be emitted downstream. ([#29600](https://github.com/apache/beam/issues/29600)).
* Some Python pipelines that run with 2.52.0-2.54.0 SDKs and use large materialized side inputs might be affected by a performance regression. To restore the prior behavior on these SDK versions, supply the `--max_cache_memory_usage_mb=0` pipeline option. (Python) ([#30360](https://github.com/apache/beam/issues/30360)).
* Users who lauch Python pipelines in an environment without internet access and use the `--setup_file` pipeline option might experience an increase in pipeline submission time. This has been fixed in 2.56.0 ([#31070](https://github.com/apache/beam/pull/31070)).
* Transforms which use `SnappyCoder` are update incompatible with previous versions of the same transform (Java) on some runners. This includes PubSubIO's read ([#28655](https://github.com/apache/beam/pull/28655#issuecomment-2407839769)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.52.0 release. Thank you to all contributors!

Ahmed Abualsaud

Ahmet Altay

Aleksandr Dudko

Alexey Romanenko

Anand Inguva

Andrei Gurau

Andrey Devyatkin

BjornPrime

Bruno Volpato

Bulat

Chamikara Jayalath

Damon

Danny McCormick

Devansh Modi

Dominik Dębowczyk

Ferran Fernández Garrido

Hai Joey Tran

Israel Herraiz

Jack McCluskey

Jan Lukavský

JayajP

Jeff Kinard

Jeffrey Kinard

Jiangjie Qin

Jing

Joar Wandborg

Johanna Öjeling

Julien Tournay

Kanishk Karanawat

Kenneth Knowles

Kerry Donny-Clark

Luís Bianchin

Minbo Bae

Pranav Bhandari

Rebecca Szper

Reuven Lax

Ritesh Ghorse

Robert Bradshaw

Robert Burke

RyuSA

Shunping Huang

Steven van Rossum

Svetak Sundhar

Tony Tang

Vitaly Terentyev

Vivek Sumanth

Vlado Djerek

Yi Hu

aku019

brucearctor

caneff

damccorm

ddebowczyk92

dependabot[bot]

dpcollins-google

edman124

gabry.wu

illoise

johnjcasey

jonathan-lemos

kennknowles

liferoad

magicgoody

martin trieu

nancyxu123

pablo rodriguez defino

tvalentyn

