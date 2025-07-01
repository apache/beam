---
title:  "Apache Beam 2.66.0"
date:   2025-07-01 15:00:00 -0500
categories:
  - blog
  - release
authors:
  - vterentev
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

We are happy to present the new 2.66.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2660-2025-07-01) for this release.

<!--more-->

For more information on changes in 2.66.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/30?closed=1).

## Beam 3.0.0 Development Highlights

* [Java] Java 8 support is now deprecated. It is still supported until Beam 3.
  From now, pipeline submitted by Java 8 client uses Java 11 SDK container for
  remote pipeline execution ([35064](https://github.com/apache/beam/pull/35064)).

## Highlights

* [Python] Several quality-of-life improvements to the vLLM model handler. If you use Beam RunInference with vLLM model handlers, we strongly recommend updating past this release.

### I/Os

* [IcebergIO] Now available with Beam SQL! ([#34799](https://github.com/apache/beam/pull/34799))
* [IcebergIO] Support reading with column pruning ([#34856](https://github.com/apache/beam/pull/34856))
* [IcebergIO] Support reading with pushdown filtering ([#34827](https://github.com/apache/beam/pull/34827))
* [IcebergIO] Create tables with a specified partition spec ([#34966](https://github.com/apache/beam/pull/34966), [#35268](https://github.com/apache/beam/pull/35268))
* [IcebergIO] Dynamically create namespaces if needed ([#35228](https://github.com/apache/beam/pull/35228))

### New Features / Improvements
* [Beam SQL] Introducing Beam Catalogs ([#35223](https://github.com/apache/beam/pull/35223))
* Adding Google Storage Requests Pays feature (Golang)([#30747](https://github.com/apache/beam/issues/30747)).
* [Python] Prism runner now auto-enabled for some Python pipelines using the direct runner ([#34921](https://github.com/apache/beam/pull/34921)).
* [YAML] WriteToTFRecord and ReadFromTFRecord Beam YAML support
* Python: Added JupyterLab 4.x extension compatibility for enhanced notebook integration ([#34495](https://github.com/apache/beam/pull/34495)).

### Breaking Changes

* Yapf version upgraded to 0.43.0 for formatting (Python) ([#34801](https://github.com/apache/beam/pull/34801/)).
* Python: Added JupyterLab 4.x extension compatibility for enhanced notebook integration ([#34495](https://github.com/apache/beam/pull/34495)).
* Python: Argument abbreviation is no longer enabled within Beam. If you previously abbreviated arguments (e.g. `--r` for `--runner`), you will now need to specify the whole argument ([#34934](https://github.com/apache/beam/pull/34934)).
* Java: Users of ReadFromKafkaViaSDF transform might encounter pipeline graph compatibility issues when updating the pipeline. To mitigate, set the `updateCompatibilityVersion` option to the SDK version used for the original pipeline, example `--updateCompatabilityVersion=2.64.0`
* Python: Updated `AlloyDBVectorWriterConfig` API to align with new `PostgresVectorWriter` transform. Heres a quick guide to update your code: ([#35225](https://github.com/apache/beam/issues/35225))

### Bugfixes

* (Java) Fixed CassandraIO ReadAll does not let a pipeline handle or retry exceptions ([#34191](https://github.com/apache/beam/pull/34191)).
* [Python] Fixed vLLM model handlers breaking Beam logging. ([#35053](https://github.com/apache/beam/pull/35053)).
* [Python] Fixed vLLM connection leaks that caused a throughput bottleneck and underutilization of GPU ([#35053](https://github.com/apache/beam/pull/35053)).
* [Python] Fixed vLLM server recovery mechanism in the event of a process termination ([#35234](https://github.com/apache/beam/pull/35234)).
* (Python) Fixed cloudpickle overwriting class states every time loading a same object of dynamic class ([#35062](https://github.com/apache/beam/issues/35062)).
* [Python] Fixed pip install apache-beam[interactive] causes crash on google colab ([#35148](https://github.com/apache/beam/pull/35148)).
* [IcebergIO] Fixed Beam <-> Iceberg conversion logic for arrays of structs and maps of structs ([#35230](https://github.com/apache/beam/pull/35230)).

### Known Issues

N/A

## List of Contributors

According to git shortlog, the following people contributed to the 2.66.0 release. Thank you to all contributors!

Aditya Yadav, Adrian Stoll, Ahmed Abualsaud, Bhargavkonidena, Chamikara Jayalath, Charles Nguyen, Chenzo, Damon, Danny McCormick, Derrick Williams, Enrique Calderon, Hai Joey Tran, Jack McCluskey, Kenneth Knowles, Leonardo Cesar Borges, Michael Gruschke, Minbo Bae, Minh Son Nguyen, Niel Markwick, Radosław Stankiewicz, Rakesh Kumar, Robert Bradshaw, S. Veyrié, Sam Whittle, Shubham Jaiswal, Shunping Huang, Steven van Rossum, Tanu Sharma, Vardhan Thigle, Vitaly Terentyev, XQ Hu, Yi Hu, akashorabek, atask-g, atognolag, bullet03, changliiu, claudevdm, fozzie15, ikarapanca, kristynsmith, Pablo Rodriguez Defino, tvalentyn, twosom, wollowizard
