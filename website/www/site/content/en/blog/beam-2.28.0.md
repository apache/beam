---
title:  "Apache Beam 2.28.0"
date:   2021-02-22 12:00:00 -0800
categories:
  - blog
  - release
authors:
  - chamikara
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
We are happy to present the new 2.28.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2280-2021-02-22) for this release.
For more information on changes in 2.28.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12349499).

## Highlights
* Many improvements related to Parquet support ([BEAM-11460](https://issues.apache.org/jira/browse/BEAM-11460), [BEAM-8202](https://issues.apache.org/jira/browse/BEAM-8202), and [BEAM-11526](https://issues.apache.org/jira/browse/BEAM-11526))
* Hash Functions in BeamSQL ([BEAM-10074](https://issues.apache.org/jira/browse/BEAM-10074))
* Hash functions in ZetaSQL ([BEAM-11624](https://issues.apache.org/jira/browse/BEAM-11624))
* Create ApproximateDistinct using HLL Impl ([BEAM-10324](https://issues.apache.org/jira/browse/BEAM-10324))

## I/Os
* SpannerIO supports using BigDecimal for Numeric fields ([BEAM-11643](https://issues.apache.org/jira/browse/BEAM-11643))
* Add Beam schema support to ParquetIO ([BEAM-11526](https://issues.apache.org/jira/browse/BEAM-11526))
* Support ParquetTable Writer ([BEAM-8202](https://issues.apache.org/jira/browse/BEAM-8202))
* GCP BigQuery sink (streaming inserts) uses runner determined sharding ([BEAM-11408](https://issues.apache.org/jira/browse/BEAM-11408))
* PubSub support types: TIMESTAMP, DATE, TIME, DATETIME ([BEAM-11533](https://issues.apache.org/jira/browse/BEAM-11533))

## New Features / Improvements
* ParquetIO add methods _readGenericRecords_ and _readFilesGenericRecords_ can read files with an unknown schema. See [PR-13554](https://github.com/apache/beam/pull/13554) and ([BEAM-11460](https://issues.apache.org/jira/browse/BEAM-11460))
* Added support for thrift in KafkaTableProvider ([BEAM-11482](https://issues.apache.org/jira/browse/BEAM-11482))
* Added support for HadoopFormatIO to skip key/value clone ([BEAM-11457](https://issues.apache.org/jira/browse/BEAM-11457))
* Support Conversion to GenericRecords in Convert.to transform ([BEAM-11571](https://issues.apache.org/jira/browse/BEAM-11571)).
* Support writes for Parquet Tables in Beam SQL ([BEAM-8202](https://issues.apache.org/jira/browse/BEAM-8202)).
* Support reading Parquet files with unknown schema ([BEAM-11460](https://issues.apache.org/jira/browse/BEAM-11460))
* Support user configurable Hadoop Configuration flags for ParquetIO ([BEAM-11527](https://issues.apache.org/jira/browse/BEAM-11527))
* Expose commit_offset_in_finalize and timestamp_policy to ReadFromKafka ([BEAM-11677](https://issues.apache.org/jira/browse/BEAM-11677))
* S3 options does not provided to boto3 client while using FlinkRunner and Beam worker pool container ([BEAM-11799](https://issues.apache.org/jira/browse/BEAM-11799))
* HDFS not deduplicating identical configuration paths ([BEAM-11329](https://issues.apache.org/jira/browse/BEAM-11329))
* Hash Functions in BeamSQL ([BEAM-10074](https://issues.apache.org/jira/browse/BEAM-10074))
* Create ApproximateDistinct using HLL Impl ([BEAM-10324](https://issues.apache.org/jira/browse/BEAM-10324))
* Add Beam schema support to ParquetIO ([BEAM-11526](https://issues.apache.org/jira/browse/BEAM-11526))
* Add a Deque Encoder ([BEAM-11538](https://issues.apache.org/jira/browse/BEAM-11538))
* Hash functions in ZetaSQL ([BEAM-11624](https://issues.apache.org/jira/browse/BEAM-11624))
* Refactor ParquetTableProvider ([](https://issues.apache.org/jira/browse/))
* Add JVM properties to JavaJobServer ([BEAM-8344](https://issues.apache.org/jira/browse/BEAM-8344))
* Single source of truth for supported Flink versions ([](https://issues.apache.org/jira/browse/))
* Use metric for Python BigQuery streaming insert API latency logging ([BEAM-11018](https://issues.apache.org/jira/browse/BEAM-11018))
* Use metric for Java BigQuery streaming insert API latency logging ([BEAM-11032](https://issues.apache.org/jira/browse/BEAM-11032))
* Upgrade Flink runner to Flink versions 1.12.1 and 1.11.3 ([BEAM-11697](https://issues.apache.org/jira/browse/BEAM-11697))
* Upgrade Beam base image to use Tensorflow 2.4.1 ([BEAM-11762](https://issues.apache.org/jira/browse/BEAM-11762))
* Create Beam GCP BOM ([BEAM-11665](https://issues.apache.org/jira/browse/BEAM-11665))

## Breaking Changes

* The Java artifacts "beam-sdks-java-io-kinesis", "beam-sdks-java-io-google-cloud-platform", and
  "beam-sdks-java-extensions-sql-zetasql" declare Guava 30.1-jre dependency (It was 25.1-jre in Beam 2.27.0).
  This new Guava version may introduce dependency conflicts if your project or dependencies rely
  on removed APIs. If affected, ensure to use an appropriate Guava version via `dependencyManagement` in Maven and
  `force` in Gradle.

## List of Contributors

According to git shortlog, the following people contributed to the 2.28.0 release. Thank you to all contributors!

Ahmet Altay, Alex Amato, Alexey Romanenko, Allen Pradeep Xavier, Anant Damle, Artur Khanin,
Boyuan Zhang, Brian Hulette, Chamikara Jayalath, Chris Roth, Costi Ciudatu, Damon Douglas,
Daniel Collins, Daniel Oliveira, David Cavazos, David Huntsperger, Elliotte Rusty Harold,
Emily Ye, Etienne Chauchot, Etta Rapp, Evan Palmer, Eyal, Filip Krakowski, Fokko Driesprong,
Heejong Lee, Ismaël Mejía, janeliulwq, Jan Lukavský, John Edmonds, Jozef Vilcek, Kenneth Knowles
Ke Wu, kileys, Kyle Weaver, MabelYC, masahitojp, Masato Nakamura, Milena Bukal, Miraç Vuslat Başaran,
Nelson Osacky, Niel Markwick, Ning Kang, omarismail94, Pablo Estrada, Piotr Szuberski,
ramazan-yapparov, Reuven Lax, Reza Rokni, rHermes, Robert Bradshaw, Robert Burke, Robert Gruener,
Romster, Rui Wang, Sam Whittle, shehzaadn-vd, Siyuan Chen, Sonam Ramchand, Tobiasz Kędzierski,
Tomo Suzuki, tszerszen, tvalentyn, Tyson Hamilton, Udi Meiri, Xinbin Huang, Yichi Zhang,
Yifan Mai, yoshiki.obata, Yueyang Qiu, Yusaku Matsuki
