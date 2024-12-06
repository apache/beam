---
title:  "Apache Beam 2.59.0"
date:   2024-09-11 13:00:00 -0800
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

We are happy to present the new 2.59.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2590-2024-09-11) for this release.

<!--more-->

For more information on changes in 2.59.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/23).

## Highlights

* Added support for setting a configureable timeout when loading a model and performing inference in the [RunInference](https://beam.apache.org/documentation/ml/inference-overview/) transform using [with_exception_handling](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.RunInference.with_exception_handling) ([#32137](https://github.com/apache/beam/issues/32137))
* Initial experimental support for using [Prism](/documentation/runners/prism/) with the Java and Python SDKs
  * Prism is presently targeting local testing usage, or other small scale execution.
  * For Java, use 'PrismRunner', or 'TestPrismRunner' as an argument to the `--runner` flag.
  * For Python, use 'PrismRunner' as an argument to the `--runner` flag.
  * Go already uses Prism as the default local runner.

## I/Os

* Improvements to the performance of BigqueryIO when using withPropagateSuccessfulStorageApiWrites(true) method (Java) ([#31840](https://github.com/apache/beam/pull/31840)).
* [Managed Iceberg] Added support for writing to partitioned tables ([#32102](https://github.com/apache/beam/pull/32102))
* Update ClickHouseIO to use the latest version of the ClickHouse JDBC driver ([#32228](https://github.com/apache/beam/issues/32228)).
* Add ClickHouseIO dedicated User-Agent ([#32252](https://github.com/apache/beam/issues/32252)).

## New Features / Improvements

* BigQuery endpoint can be overridden via PipelineOptions, this enables BigQuery emulators (Java) ([#28149](https://github.com/apache/beam/issues/28149)).
* Go SDK Minimum Go Version updated to 1.21 ([#32092](https://github.com/apache/beam/pull/32092)).
* [BigQueryIO] Added support for withFormatRecordOnFailureFunction() for STORAGE_WRITE_API and STORAGE_API_AT_LEAST_ONCE methods (Java) ([#31354](https://github.com/apache/beam/issues/31354)).
* Updated Go protobuf package to new version (Go) ([#21515](https://github.com/apache/beam/issues/21515)).
* Added support for setting a configureable timeout when loading a model and performing inference in the [RunInference](https://beam.apache.org/documentation/ml/inference-overview/) transform using [with_exception_handling](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.inference.base.html#apache_beam.ml.inference.base.RunInference.with_exception_handling) ([#32137](https://github.com/apache/beam/issues/32137))
* Adds OrderedListState support for Java SDK via FnApi.
* Initial support for using Prism from the Python and Java SDKs.

## Bugfixes

* Fixed incorrect service account impersonation flow for Python pipelines using BigQuery IOs ([#32030](https://github.com/apache/beam/issues/32030)).
* Auto-disable broken and meaningless `upload_graph` feature when using Dataflow Runner V2 ([#32159](https://github.com/apache/beam/issues/32159)).
* (Python) Upgraded google-cloud-storage to version 2.18.2 to fix a data corruption issue ([#32135](https://github.com/apache/beam/pull/32135)).
* (Go) Fix corruption on State API writes. ([#32245](https://github.com/apache/beam/issues/32245)).

## Known Issues

* Prism is under active development and does not yet support all pipelines. See [#29650](https://github.com/apache/beam/issues/29650) for progress.
   * In the 2.59.0 release, Prism passes most runner validations tests with the exceptions of pipelines using the following features:
   OrderedListState, OnWindowExpiry (eg. GroupIntoBatches), CustomWindows, MergingWindowFns, Trigger and WindowingStrategy associated features, Bundle Finalization, Looping Timers, and some Coder related issues such as with Python combiner packing, and Java Schema transforms, and heterogenous flatten coders. Processing Time timers do not yet have real time support.
   * If your pipeline is having difficulty with the Python or Java direct runners, but runs well on Prism, please let us know.
* Java file-based IOs read or write lots (100k+) files could experience slowness and/or broken metrics visualization on Dataflow UI [#32649](https://github.com/apache/beam/issues/32649).
* BigQuery Enrichment (Python):  The following issues are present when using the BigQuery enrichment transform ([#32780](https://github.com/apache/beam/pull/32780)):
  * Duplicate Rows: Multiple conditions may be applied incorrectly, leading to the duplication of rows in the output.
  * Incorrect Results with Batched Requests: Conditions may not be correctly scoped to individual rows within the batch, potentially causing inaccurate results.
  * Fixed in 2.61.0.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.59.0 release. Thank you to all contributors!

Ahmed Abualsaud,Ahmet Altay,Andrew Crites,atask-g,Axel Magnuson,Ayush Pandey,Bartosz Zablocki,Chamikara Jayalath,cutiepie-10,Damon,Danny McCormick,dependabot[bot],Eddie Phillips,Francis O'Hara,Hyeonho Kim,Israel Herraiz,Jack McCluskey,Jaehyeon Kim,Jan Lukavský,Jeff Kinard,Jeffrey Kinard,jonathan-lemos,jrmccluskey,Kirill Berezin,Kiruphasankaran Nataraj,lahariguduru,liferoad,lostluck,Maciej Szwaja,Manit Gupta,Mark Zitnik,martin trieu,Naireen Hussain,Prerit Chandok,Radosław Stankiewicz,Rebecca Szper,Robert Bradshaw,Robert Burke,ron-gal,Sam Whittle,Sergei Lilichenko,Shunping Huang,Svetak Sundhar,Thiago Nunes,Timothy Itodo,tvalentyn,twosom,Vatsal,Vitaly Terentyev,Vlado Djerek,Yifan Ye,Yi Hu
