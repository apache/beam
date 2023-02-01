---
title:  "Apache Beam 2.38.0"
date:   2022-04-20 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - danoliveira
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

We are happy to present the new 2.38.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2380-2022-04-20) for this release.

<!--more-->

For more information on changes in 2.38.0 check out the [detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12351169).

## I/Os
* Introduce projection pushdown optimizer to the Java SDK ([BEAM-12976](https://issues.apache.org/jira/browse/BEAM-12976)). The optimizer currently only works on the [BigQuery Storage API](/documentation/io/built-in/google-bigquery/#storage-api), but more I/Os will be added in future releases. If you encounter a bug with the optimizer, please file a JIRA and disable the optimizer using pipeline option `--experiments=disable_projection_pushdown`.
* A new IO for Neo4j graph databases was added. ([BEAM-1857](https://issues.apache.org/jira/browse/BEAM-1857))  It has the ability to update nodes and relationships using UNWIND statements and to read data using cypher statements with parameters.
* `amazon-web-services2` has reached feature parity and is finally recommended over the earlier `amazon-web-services` and `kinesis` modules (Java). These will be deprecated in one of the next releases ([BEAM-13174](https://issues.apache.org/jira/browse/BEAM-13174)).
  * Long outstanding write support for `Kinesis` was added ([BEAM-13175](https://issues.apache.org/jira/browse/BEAM-13175)).
  * Configuration was simplified and made consistent across all IOs, including the usage of `AwsOptions` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563), [BEAM-13663](https://issues.apache.org/jira/browse/BEAM-13663), [BEAM-13587](https://issues.apache.org/jira/browse/BEAM-13587)).
  * Additionally, there's a long list of recent improvements and fixes to
    `S3` Filesystem ([BEAM-13245](https://issues.apache.org/jira/browse/BEAM-13245), [BEAM-13246](https://issues.apache.org/jira/browse/BEAM-13246), [BEAM-13441](https://issues.apache.org/jira/browse/BEAM-13441), [BEAM-13445](https://issues.apache.org/jira/browse/BEAM-13445), [BEAM-14011](https://issues.apache.org/jira/browse/BEAM-14011)),
    `DynamoDB` IO ([BEAM-13209](https://issues.apache.org/jira/browse/BEAM-13009), [BEAM-13209](https://issues.apache.org/jira/browse/BEAM-13209)),
    `SQS` IO ([BEAM-13631](https://issues.apache.org/jira/browse/BEAM-13631), [BEAM-13510](https://issues.apache.org/jira/browse/BEAM-13510)) and others.

## New Features / Improvements

* Pipeline dependencies supplied through `--requirements_file` will now be staged to the runner using binary distributions (wheels) of the PyPI packages for linux_x86_64 platform ([BEAM-4032](https://issues.apache.org/jira/browse/BEAM-4032)). To restore the behavior to use source distributions, set pipeline option `--requirements_cache_only_sources`. To skip staging the packages at submission time, set pipeline option `--requirements_cache=skip` (Python).
* The Flink runner now supports Flink 1.14.x ([BEAM-13106](https://issues.apache.org/jira/browse/BEAM-13106)).
* Interactive Beam now supports remotely executing Flink pipelines on Dataproc (Python) ([BEAM-14071](https://issues.apache.org/jira/browse/BEAM-14071)).

## Breaking Changes

* (Python) Previously `DoFn.infer_output_types` was expected to return `Iterable[element_type]` where `element_type` is the PCollection elemnt type. It is now expected to return `element_type`. Take care if you have overriden `infer_output_type` in a `DoFn` (this is not common). See [BEAM-13860](https://issues.apache.org/jira/browse/BEAM-13860).
* (`amazon-web-services2`) The types of `awsRegion` / `endpoint` in `AwsOptions` changed from String to `Region` / `URI` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563)).

## Deprecations

* Beam 2.38.0 will be the last minor release to support Flink 1.11.
* (`amazon-web-services2`) Client providers (`withXYZClientProvider()`) as well as IO specific `RetryConfiguration`s are deprecated, instead use `withClientConfiguration()` or `AwsOptions` to configure AWS IOs / clients.
  Custom implementations of client providers shall be replaced with a respective `ClientBuilderFactory` and configured through `AwsOptions` ([BEAM-13563](https://issues.apache.org/jira/browse/BEAM-13563)).

## Bugfixes

* Fix S3 copy for large objects (Java) ([BEAM-14011](https://issues.apache.org/jira/browse/BEAM-14011))
* Fix quadratic behavior of pipeline canonicalization (Go) ([BEAM-14128](https://issues.apache.org/jira/browse/BEAM-14128))
  * This caused unnecessarily long pre-processing times before job submission for large complex pipelines.
* Fix `pyarrow` version parsing (Python)([BEAM-14235](https://issues.apache.org/jira/browse/BEAM-14235))

### Known Issues

* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.38.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.

## List of Contributors

According to git shortlog, the following people contributed to the 2.38.0 release. Thank you to all contributors!

abhijeet-lele
Ahmet Altay
akustov
Alexander
Alexander Zhuravlev
Alexey Romanenko
AlikRodriguez
Anand Inguva
andoni-guzman
andreukus
Andy Ye
Ankur Goenka
ansh0l
Artur Khanin
Aydar Farrakhov
Aydar Zainutdinov
Benjamin Gonzalez
Brian Hulette
brucearctor
bulat safiullin
bullet03
Carl Mastrangelo
Chamikara Jayalath
Chun Yang
Daniela Martín
Daniel Oliveira
Danny McCormick
daria.malkova
David Cavazos
David Huntsperger
dmitryor
Dmytro Sadovnychyi
dpcollins-google
egalpin
Elias Segundo Antonio
emily
Etienne Chauchot
Hengfeng Li
Ismaël Mejía
Israel Herraiz
Jack McCluskey
Jakub Kukul
Janek Bevendorff
Jeff Klukas
Johan Sternby
Kamil Breguła
Kenneth Knowles
Ke Wu
Kiley
Kyle Weaver
laraschmidt
Lara Schmidt
LE QUELLEC Olivier
Luka Kalinovcic
Luke Cwik
Marcin Kuthan
masahitojp
Masato Nakamura
Matt Casters
Melissa Pashniak
Michael Li
Miguel Hernandez
Moritz Mack
mosche
nancyxu123
Nathan J Mehl
Niel Markwick
Ning Kang
Pablo Estrada
paul-tlh
Pavel Avilov
Rahul Iyer
Reuven Lax
Ritesh Ghorse
Robert Bradshaw
Robert Burke
Ryan Skraba
Ryan Thompson
Sam Whittle
Seth Vargo
sp029619
Steven Niemitz
Thiago Nunes
Udi Meiri
Valentyn Tymofieiev
Victor
vitaly.terentyev
Yichi Zhang
Yi Hu
yirutang
Zachary Houfek
Zoe
