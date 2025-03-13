---
title:  "Apache Beam 2.34.0"
date:   2021-11-11 00:11:00 -0800
categories:
  - blog
  - release
authors:
  - ibzib
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

We are happy to present the new 2.34.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2340-2021-11-11) for this release.

<!--more-->

For more information on changes in 2.34.0, check out the [detailed release
notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12350405).

## Highlights

* The Beam Java API for Calcite SqlTransform is no longer experimental ([BEAM-12680](https://issues.apache.org/jira/browse/BEAM-12680)).
* Python's ParDo (Map, FlatMap, etc.) transforms now suport a `with_exception_handling` option for easily ignoring bad records and implementing the dead letter pattern.

## I/Os

* `ReadFromBigQuery` and `ReadAllFromBigQuery` now run queries with BATCH priority by default. The `query_priority` parameter is introduced to the same transforms to allow configuring the query priority (Python) ([BEAM-12913](https://issues.apache.org/jira/browse/BEAM-12913)).
* [EXPERIMENTAL] Support for [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage) added to `ReadFromBigQuery`. The newly introduced `method` parameter can be set as `DIRECT_READ` to use the Storage Read API. The default is `EXPORT` which invokes a BigQuery export request. (Python) ([BEAM-10917](https://issues.apache.org/jira/browse/BEAM-10917)).
* [EXPERIMENTAL] Added `use_native_datetime` parameter to `ReadFromBigQuery` to configure the return type of [DATETIME](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#datetime_type) fields when using `ReadFromBigQuery`. This parameter can *only* be used when `method = DIRECT_READ`(Python) ([BEAM-10917](https://issues.apache.org/jira/browse/BEAM-10917)).

## New Features / Improvements

* Upgrade to Calcite 1.26.0 ([BEAM-9379](https://issues.apache.org/jira/browse/BEAM-9379)).
* Added a new `dataframe` extra to the Python SDK that tracks `pandas` versions
  we've verified compatibility with. We now recommend installing Beam with `pip
  install apache-beam[dataframe]` when you intend to use the DataFrame API
  ([BEAM-12906](https://issues.apache.org/jira/browse/BEAM-12906)).
* Add an [example](https://github.com/cometta/python-apache-beam-spark) of deploying Python Apache Beam job with Spark Cluster

## Breaking Changes

* SQL Rows are no longer flattened ([BEAM-5505](https://issues.apache.org/jira/browse/BEAM-5505)).
* [Go SDK] beam.TryCrossLanguage's signature now matches beam.CrossLanguage. Like other Try functions it returns an error instead of panicking. ([BEAM-9918](https://issues.apache.org/jira/browse/BEAM-9918)).
* [BEAM-12925](https://jira.apache.org/jira/browse/BEAM-12925) was fixed. It used to silently pass incorrect null data read from JdbcIO. Pipelines affected by this will now start throwing failures instead of silently passing incorrect data.

## Bugfixes

* Fixed error while writing multiple DeferredFrames to csv (Python) ([BEAM-12701](https://issues.apache.org/jira/browse/BEAM-12701)).
* Fixed error when importing the DataFrame API with pandas 1.0.x installed ([BEAM-12945](https://issues.apache.org/jira/browse/BEAM-12945)).
* Fixed top.SmallestPerKey implementation in the Go SDK ([BEAM-12946](https://issues.apache.org/jira/browse/BEAM-12946)).

### Known Issues

* Large Java BigQueryIO writes with the FILE_LOADS method will fail in batch mode (specifically, when copy jobs are used).
  This results in the error message: `IllegalArgumentException: Attempting to access unknown side input`.
  Please upgrade to a newer version (> 2.34.0) or use another write method (e.g. `STORAGE_WRITE_API`).

## List of Contributors

According to git shortlog, the following people contributed to the 2.34.0 release. Thank you to all contributors!

Ahmet Altay,
Aizhamal Nurmamat kyzy,
Alex Amato,
Alexander Chermenin,
Alexey Romanenko,
AlikRodriguez,
Andrew Pilloud,
Andy Xu,
Ankur Goenka,
Aydar Farrakhov,
Aydar Zainutdinov,
Aydar Zaynutdinov,
AydarZaynutdinov,
Benjamin Gonzalez,
BenWhitehead,
Brachi Packter,
Brian Hulette,
Bu Sun Kim,
Chamikara Jayalath,
Chris Gray,
Chuck Yang,
Chun Yang,
Claire McGinty,
comet,
Daniel Collins,
Daniel Oliveira,
Daniel Thevessen,
daria.malkova,
David Cavazos,
David Huntsperger,
Dmytro Kozhevin,
dpcollins-google,
Eduardo Sánchez López,
Elias Djurfeldt,
emily,
Emily Ye,
Enis Sert,
Etienne Chauchot,
Fernando Morales,
Heejong Lee,
Ihor Indyk,
Ismaël Mejía,
Israel Herraiz,
Jack McCluskey,
Jonathan Hourany,
Judah Rand,
Kenneth Knowles,
KevinGG,
Ke Wu,
kileys,
Kyle Weaver,
Luke Cwik,
masahitojp,
MiguelAnzoWizeline,
Minbo Bae,
Niels Basjes,
Ning Kang,
Pablo Estrada,
pareshsarafmdb,
Paul Féraud,
Piotr Szczepanik,
Reuven Lax,
Ritesh Ghorse,
R. Miles McCain,
Robert Bradshaw,
Robert Burke,
Rogan Morrow,
Ruwan Lambrichts,
rvballada,
Ryan Thompson,
Sam Rohde,
Sam Whittle,
Ștefan Istrate,
Steve Niemitz,
Thomas Li Fredriksen,
Tomo Suzuki,
tvalentyn,
Udi Meiri,
Vachan,
Valentyn Tymofieiev,
Vincent Marquez,
WinsonT,
Yichi Zhang,
Yifan Mai,
Yilei "Dolee" Yang,
zhoufek
