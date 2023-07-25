---
title:  "Apache Beam 2.35.0"
date:   2021-12-29 10:11:00 -0800
categories:
  - blog
  - release
authors:
  - tvalentyn
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

We are happy to present the new 2.35.0 release of Apache Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2350-2021-12-29) for this release.

<!--more-->

For more information on changes in 2.35.0, check out the [detailed release
notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12350406).

## Highlights

* MultiMap side inputs are now supported by the Go SDK ([BEAM-3293](https://issues.apache.org/jira/browse/BEAM-3293)).
* Side inputs are supported within Splittable DoFns for Dataflow Runner V1 and Dataflow Runner V2. ([BEAM-12522](https://issues.apache.org/jira/browse/BEAM-12522)).
* Upgrades Log4j version used in test suites (Apache Beam testing environment only, not for end user consumption) to 2.17.0([BEAM-13434](https://issues.apache.org/jira/browse/BEAM-13434)).
    Note that Apache Beam versions do not depend on the Log4j 2 dependency (log4j-core) impacted by [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228).
    However we urge users to update direct and indirect dependencies (if any) on Log4j 2 to the latest version by updating their build configuration and redeploying impacted pipelines.

## I/Os

* We changed the data type for ranges in `JdbcIO.readWithPartitions` from `int` to `long` ([BEAM-13149](https://issues.apache.org/jira/browse/BEAM-13149)).
    This is a relatively minor breaking change, which we're implementing to improve the usability of the transform without increasing cruft.
    This transform is relatively new, so we may implement other breaking changes in the future to improve its usability.
* Side inputs are supported within Splittable DoFns for Dataflow Runner V1 and Dataflow Runner V2. ([BEAM-12522](https://issues.apache.org/jira/browse/BEAM-12522)).

## New Features / Improvements

* Added custom delimiters to Python TextIO reads ([BEAM-12730](https://issues.apache.org/jira/browse/BEAM-12730)).
* Added escapechar parameter to Python TextIO reads ([BEAM-13189](https://issues.apache.org/jira/browse/BEAM-13189)).
* Splittable reading is enabled by default while reading data with ParquetIO ([BEAM-12070](https://issues.apache.org/jira/browse/BEAM-12070)).
* DoFn Execution Time metrics added to Go ([BEAM-13001](https://issues.apache.org/jira/browse/BEAM-13001)).
* Cross-bundle side input caching is now available in the Go SDK for runners that support the feature by setting the EnableSideInputCache hook ([BEAM-11097](https://issues.apache.org/jira/browse/BEAM-11097)).
* Upgraded the GCP Libraries BOM version to 24.0.0 and associated dependencies ([BEAM-11205](
  https://issues.apache.org/jira/browse/BEAM-11205)). For Google Cloud client library versions set by this BOM,
  see [this table](https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/24.0.0/artifact_details.html).
* Removed avro-python3 dependency in AvroIO. Fastavro has already been our Avro library of choice on Python 3. Boolean use_fastavro is left for api compatibility, but will have no effect.([BEAM-13016](https://github.com/apache/beam/pull/15900)).
* MultiMap side inputs are now supported by the Go SDK ([BEAM-3293](https://issues.apache.org/jira/browse/BEAM-3293)).
* Remote packages can now be downloaded from locations supported by apache_beam.io.filesystems. The files will be downloaded on Stager and uploaded to staging location. For more information, see [BEAM-11275](https://issues.apache.org/jira/browse/BEAM-11275)

## Breaking Changes

* A new URN convention was adopted for cross-language transforms and existing URNs were updated. This may break advanced use-cases, for example, if a custom expansion service is used to connect diffrent Beam Java and Python versions. ([BEAM-12047](https://issues.apache.org/jira/browse/BEAM-12047)).
* The upgrade to Calcite 1.28.0 introduces a breaking change in the SUBSTRING function in SqlTransform, when used with the Calcite dialect ([BEAM-13099](https://issues.apache.org/jira/browse/BEAM-13099), [CALCITE-4427](https://issues.apache.org/jira/browse/CALCITE-4427)).
* ListShards (with DescribeStreamSummary) is used instead of DescribeStream to list shards in Kinesis streams (AWS SDK v2). Due to this change, as mentioned in [AWS documentation](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html), for fine-grained IAM policies it is required to update them to allow calls to ListShards and DescribeStreamSummary APIs. For more information, see [Controlling Access to Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) ([BEAM-13233](https://issues.apache.org/jira/browse/BEAM-13233)).

## Deprecations

* Non-splittable reading is deprecated while reading data with ParquetIO ([BEAM-12070](https://issues.apache.org/jira/browse/BEAM-12070)).

## Bugfixes

* Properly map main input windows to side input windows by default (Go)
  ([BEAM-11087](https://issues.apache.org/jira/browse/BEAM-11087)).
* Fixed data loss when writing to DynamoDB without setting deduplication key names (Java)
  ([BEAM-13009](https://issues.apache.org/jira/browse/BEAM-13009)).
* Go SDK Examples now have types and functions registered. (Go) ([BEAM-5378](https://issues.apache.org/jira/browse/BEAM-5378))
* Fixed data loss when using Python WriteToFiles in streaming pipeline ([BEAM-12950](https://issues.apache.org/jira/browse/BEAM-12950)).

## Known Issues

* Users of beam-sdks-java-io-hcatalog (and beam-sdks-java-extensions-sql-hcatalog) must take care to override the transitive log4j dependency when they add a hive dependency ([BEAM-13499](https://issues.apache.org/jira/browse/BEAM-13499)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.35.0 release. Thank you to all contributors!

Ahmet Altay
Alexandr Zhuravlev
Alexey Romanenko
AlikRodriguez
Anand Inguva
Andrew Pilloud
Ankur Goenka
Anthony Sottile
Artur Khanin
Aydar Farrakhov
Aydar Zainutdinov
Benjamin Gonzalez
brachipa
Brian Hulette
Calvin Leung
Chamikara Jayalath
Chris Gray
Damon Douglas
Daniel Collins
Daniel Oliveira
daria.malkova
darshan-sj
David Huntsperger
David Prieto Rivera
Dmitrii Kuzin
dpcollins-google
dprieto
egalpin
Etienne Chauchot
Eugene Nikolaiev
Fernando Morales
Hector Lagos
Heejong Lee
Ilya Kozyrev
Iñigo San Jose Visiers
Jack McCluskey
Jiayang Wu
jrhy
Kenneth Knowles
KevinGG
kileys
klmilam
Kyle Weaver
Luís Bianchin
Luke Cwik
Melissa Pashniak
Michael Luckey
Miguel Hernandez
Milena Bukal
Minbo Bae
minherz
Moritz Mack
mosche
Natalie
Ning Kang
Pablo Estrada
Pavel Avilov
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Rogan Morrow
Ruslan Altynnikov
Sam Whittle
Sergey Kalinin
Slava Chernyak
Svetak Sundhar
Tianyang Hu
Tim Robertson
Tomo Suzuki
tuorhador
Udi Meiri
vachan-shetty
Valentyn Tymofieiev
Yichi Zhang
zhoufek
