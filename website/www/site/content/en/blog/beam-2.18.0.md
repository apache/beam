---
title:  "Apache Beam 2.18.0"
date:   2020-01-23 00:00:01 -0800
categories:
  - blog
# Date above corrected but keep the old URL:
aliases:
  - /blog/2020/01/13/beam-2.18.0.html
authors:
  - udim
  - altay

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

We are happy to present the new 2.18.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2180-2020-01-23) for this release.<!--more-->
For more information on changes in 2.18.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?version=12346383&projectId=12319527).

## Highlights

 * [BEAM-8470](https://issues.apache.org/jira/browse/BEAM-8470) - Create a new Spark runner based on Spark Structured streaming framework

### I/Os
* [BEAM-7636](https://issues.apache.org/jira/browse/BEAM-7636) - Added SqsIO v2 support.
* [BEAM-8513](https://issues.apache.org/jira/browse/BEAM-8513) - RabbitMqIO: Allow reads from exchange-bound queue without declaring the exchange.
* [BEAM-8540](https://issues.apache.org/jira/browse/BEAM-8540) - Fix CSVSink example in FileIO docs

### New Features / Improvements

* [BEAM-5878](https://issues.apache.org/jira/browse/BEAM-5878) - Added support DoFns with Keyword-only arguments in Python 3.
* [BEAM-6756](https://issues.apache.org/jira/browse/BEAM-6756) - Improved support for lazy iterables in schemas (Java).
* [BEAM-4776](https://issues.apache.org/jira/browse/BEAM-4776) AND [BEAM-4777](https://issues.apache.org/jira/browse/BEAM-4777) - Added metrics supports to portable runners.
* Various improvements to Interactive Beam: [BEAM-7760](https://issues.apache.org/jira/browse/BEAM-7760), [BEAM-8379](https://issues.apache.org/jira/browse/BEAM-8379), [BEAM-8016](https://issues.apache.org/jira/browse/BEAM-8016), [BEAM-8016](https://issues.apache.org/jira/browse/BEAM-8016).
* [BEAM-8658](https://issues.apache.org/jira/browse/BEAM-8658) - Optionally set artifact staging port in FlinkUberJarJobServer.
* [BEAM-8660](https://issues.apache.org/jira/browse/BEAM-8660) - Override returned artifact staging endpoint

### SQL
* [BEAM-8343](https://issues.apache.org/jira/browse/BEAM-8343) - [SQL] Add means for IO APIs to support predicate and/or project push-down when running SQL pipelines. And [BEAM-8468](https://issues.apache.org/jira/browse/BEAM-8468), [BEAM-8365](https://issues.apache.org/jira/browse/BEAM-8365), [BEAM-8508](https://issues.apache.org/jira/browse/BEAM-8508).
* [BEAM-8427](https://issues.apache.org/jira/browse/BEAM-8427) - [SQL] Add support for MongoDB source.
* [BEAM-8456](https://issues.apache.org/jira/browse/BEAM-8456) - Add pipeline option to control truncate of BigQuery data processed by Beam SQL.

### Breaking Changes

* [BEAM-8814](https://issues.apache.org/jira/browse/BEAM-8814) - --no_auth flag changed to boolean type.


### Deprecations

* [BEAM-8252](https://issues.apache.org/jira/browse/BEAM-8252) AND [BEAM-8254](https://issues.apache.org/jira/browse/BEAM-8254) Add worker_region and worker_zone options. Deprecated --zone flag and --worker_region experiment argument.

### Dependency Changes
* [BEAM-7078](https://issues.apache.org/jira/browse/BEAM-7078) - com.amazonaws:amazon-kinesis-client updated to 1.13.0.
* [BEAM-8822](https://issues.apache.org/jira/browse/BEAM-8822) - Upgrade Hadoop dependencies to version 2.8.

### Bugfixes

* [BEAM-7917](https://issues.apache.org/jira/browse/BEAM-7917) - Python datastore v1new fails on retry.
* [BEAM-7981](https://issues.apache.org/jira/browse/BEAM-7981) - ParDo function wrapper doesn't support Iterable output types.
* [BEAM-8146](https://issues.apache.org/jira/browse/BEAM-8146) - SchemaCoder/RowCoder have no equals() function.
* [BEAM-8347](https://issues.apache.org/jira/browse/BEAM-8347) - UnboundedRabbitMqReader can fail to advance watermark if no new data comes in.
* [BEAM-8352](https://issues.apache.org/jira/browse/BEAM-8352) - Reading records in background may lead to OOM errors
* [BEAM-8480](https://issues.apache.org/jira/browse/BEAM-8480) - Explicitly set restriction coder for bounded reader wrapper SDF.
* [BEAM-8515](https://issues.apache.org/jira/browse/BEAM-8515) - Ensure that ValueProvider types have equals/hashCode implemented for comparison reasons.
* [BEAM-8579](https://issues.apache.org/jira/browse/BEAM-8579) - Strip UTF-8 BOM bytes (if present) in TextSource.
* [BEAM-8657](https://issues.apache.org/jira/browse/BEAM-8657) - Not doing Combiner lifting for data-driven triggers.
* [BEAM-8663](https://issues.apache.org/jira/browse/BEAM-8663) - BundleBasedRunner Stacked Bundles don't respect PaneInfo.
* [BEAM-8667](https://issues.apache.org/jira/browse/BEAM-8667) - Data channel should to avoid unlimited buffering in Python SDK.
* [BEAM-8802](https://issues.apache.org/jira/browse/BEAM-8802) - Timestamp combiner not respected across bundles in streaming mode.
* [BEAM-8803](https://issues.apache.org/jira/browse/BEAM-8803) - Default behaviour for Python BQ Streaming inserts sink should be to retry always.
* [BEAM-8825](https://issues.apache.org/jira/browse/BEAM-8825) - OOM when writing large numbers of 'narrow' rows.
* [BEAM-8835](https://issues.apache.org/jira/browse/BEAM-8835) - Artifact retrieval fails with FlinkUberJarJobServer
* [BEAM-8836](https://issues.apache.org/jira/browse/BEAM-8836) - ExternalTransform is not providing a unique name
* [BEAM-8884](https://issues.apache.org/jira/browse/BEAM-8884) - Python MongoDBIO TypeError when splitting.
* [BEAM-9041](https://issues.apache.org/jira/browse/BEAM-9041) - SchemaCoder equals should not rely on from/toRowFunction equality.
* [BEAM-9042](https://issues.apache.org/jira/browse/BEAM-9042) - AvroUtils.schemaCoder(schema) produces a not serializable SchemaCoder.
* [BEAM-9065](https://issues.apache.org/jira/browse/BEAM-9065) - Spark runner accumulates metrics (incorrectly) between runs.
* [BEAM-6303](https://issues.apache.org/jira/browse/BEAM-6303) - Add .parquet extension to files in ParquetIO.
* Various bug fixes and performance improvements.

### Known Issues

* [BEAM-8882](https://issues.apache.org/jira/browse/BEAM-8882) - Python: `beam.Create` no longer preserves order unless `reshuffle=False` is passed in as an argument.

  You may encounter this issue when using DirectRunner.
* [BEAM-9065](https://issues.apache.org/jira/browse/BEAM-9065) - Spark runner accumulates metrics (incorrectly) between runs
* [BEAM-9123](https://issues.apache.org/jira/browse/BEAM-9123) - HadoopResourceId returns wrong directory name
* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.18.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.
* [BEAM-9144](https://issues.apache.org/jira/browse/BEAM-9144) - If you are using Avro 1.9.x with Beam you should not upgrade to this version. There is an issue with timestamp conversions. A fix will be available in the next release.


## List of Contributors

According to git shortlog, the following people contributed to the 2.18.0 release. Thank you to all contributors!

Ahmet Altay, Aizhamal Nurmamat kyzy, Alan Myrvold, Alexey Romanenko, Alex Van Boxel, Andre Araujo, Andrew Crites, Andrew Pilloud, Aryan Naraghi, Boyuan Zhang, Brian Hulette, bumblebee-coming, Cerny Ondrej, Chad Dombrova, Chamikara Jayalath, Changming Ma, Chun Yang, cmachgodaddy, Colm O hEigeartaigh, Craig Chambers, Daniel Oliveira, Daniel Robert, David Cavazos, David Moravek, David Song, dependabot[bot], Derek, Dmytro Sadovnychyi, Elliotte Rusty Harold, Etienne Chauchot, Hai Lu, Henry Suryawirawan, Ismaël Mejía, Jack Whelpton, Jan Lukavský, Jean-Baptiste Onofré, Jeff Klukas, Jincheng Sun, Jing, Jing Chen, Joe Tsai, Jonathan Alvarez-Gutierrez, Kamil Wasilewski, KangZhiDong, Kasia Kucharczyk, Kenneth Knowles, kirillkozlov, Kirill Kozlov, Kyle Weaver, liumomo315, lostluck, Łukasz Gajowy, Luke Cwik, Mark Liu, Maximilian Michels, Michal Walenia, Mikhail Gryzykhin, Niel Markwick, Ning Kang, nlofeudo, pabloem, Pablo Estrada, Pankaj Gudlani, Piotr Szczepanik, Primevenn, Reuven Lax, Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, RusOr10n, Ryan Skraba, Saikat Maitra, sambvfx, Sam Rohde, Samuel Husso, Stefano, Steve Koonce, Steve Niemitz, sunjincheng121, Thomas Weise, Tianyang Hu, Tim Robertson, Tomo Suzuki, tvalentyn, Udi Meiri, Valentyn Tymofieiev, Viola Lyu, Wenjia Liu, Yichi Zhang, Yifan Zou, yoshiki.obata, Yueyang Qiu, ziel, 康智冬
