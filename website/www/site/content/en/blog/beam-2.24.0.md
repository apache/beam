---
title:  "Apache Beam 2.24.0"
date:   2020-09-17 00:00:01 -0800
categories:
  - blog
authors:
  - Daniel Oliveira
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
We are happy to present the new 2.24.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2240-2020-09-17) for this release.
For more information on changes in 2.24.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12347146).

## Highlights

* Apache Beam 2.24.0 is the last release with Python 2 and Python 3.5
  support.

## I/Os

* New overloads for BigtableIO.Read.withKeyRange() and BigtableIO.Read.withRowFilter()
  methods that take ValueProvider as a parameter (Java) ([BEAM-10283](https://issues.apache.org/jira/browse/BEAM-10283)).
* The WriteToBigQuery transform (Python) in Dataflow Batch no longer relies on BigQuerySink by default. It relies on
  a new, fully-featured transform based on file loads into BigQuery. To revert the behavior to the old implementation,
  you may use `--experiments=use_legacy_bq_sink`.
* Add cross-language support to Java's JdbcIO, now available in the Python module `apache_beam.io.jdbc` ([BEAM-10135](https://issues.apache.org/jira/browse/BEAM-10135), [BEAM-10136](https://issues.apache.org/jira/browse/BEAM-10136)).
* Add support of AWS SDK v2 for KinesisIO.Read (Java) ([BEAM-9702](https://issues.apache.org/jira/browse/BEAM-9702)).
* Add streaming support to SnowflakeIO in Java SDK ([BEAM-9896](https://issues.apache.org/jira/browse/BEAM-9896))
* Support reading and writing to Google Healthcare DICOM APIs in Python SDK ([BEAM-10601](https://issues.apache.org/jira/browse/BEAM-10601))
* Add dispositions for SnowflakeIO.write ([BEAM-10343](https://issues.apache.org/jira/browse/BEAM-10343))
* Add cross-language support to SnowflakeIO.Read now available in the Python module `apache_beam.io.external.snowflake` ([BEAM-9897](https://issues.apache.org/jira/browse/BEAM-9897)).

## New Features / Improvements

* Shared library for simplifying management of large shared objects added to Python SDK. Example use case is sharing a large TF model object across threads ([BEAM-10417](https://issues.apache.org/jira/browse/BEAM-10417)).
* Dataflow streaming timers are not strictly time ordered when set earlier mid-bundle ([BEAM-8543](https://issues.apache.org/jira/browse/BEAM-8543)).
* OnTimerContext should not create a new one when processing each element/timer in FnApiDoFnRunner ([BEAM-9839](https://issues.apache.org/jira/browse/BEAM-9839))
* Key should be available in @OnTimer methods (Spark Runner) ([BEAM-9850](https://issues.apache.org/jira/browse/BEAM-9850))

## Breaking Changes

* WriteToBigQuery transforms now require a GCS location to be provided through either
  custom_gcs_temp_location in the constructor of WriteToBigQuery or the fallback option
  --temp_location, or pass method="STREAMING_INSERTS" to WriteToBigQuery ([BEAM-6928](https://issues.apache.org/jira/browse/BEAM-6928)).


## List of Contributors

According to git shortlog, the following people contributed to the 2.24.0 release. Thank you to all contributors!

adesormi, Ahmet Altay, Alex Amato, Alexey Romanenko, Andrew Pilloud, Ashwin Ramaswami, Borzoo,
Boyuan Zhang, Brian Hulette, Brian M, Bu Sun Kim, Chamikara Jayalath, Colm O hEigeartaigh,
Corvin Deboeser, Damian Gadomski, Damon Douglas, Daniel Oliveira, Dariusz Aniszewski,
davidak09, David Cavazos, David Moravek, David Yan, dhodun, Doug Roeper, Emil Hessman, Emily Ye,
Etienne Chauchot, Etta Rapp, Eugene Kirpichov, fuyuwei, Gleb Kanterov,
Harrison Green, Heejong Lee, Henry Suryawirawan, InigoSJ, Ismaël Mejía, Israel Herraiz,
Jacob Ferriero, Jan Lukavský, Jayendra, jfarr, jhnmora000, Jiadai Xia, JIahao wu, Jie Fan,
Jiyong Jung, Julius Almeida, Kamil Gałuszka, Kamil Wasilewski, Kasia Kucharczyk, Kenneth Knowles,
Kevin Puthusseri, Kyle Weaver, Łukasz Gajowy, Luke Cwik, Mark-Zeng, Maximilian Michels,
Michal Walenia, Niel Markwick, Ning Kang, Pablo Estrada, pawel.urbanowicz, Piotr Szuberski,
Rafi Kamal, rarokni, Rehman Murad Ali, Reuben van Ammers, Reuven Lax, Ricardo Bordon,
Robert Bradshaw, Robert Burke, Robin Qiu, Rui Wang, Saavan Nanavati, sabhyankar, Sam Rohde,
Scott Lukas, Siddhartha Thota, Simone Primarosa, Sławomir Andrian,
Steve Niemitz, Tobiasz Kędzierski, Tomo Suzuki, Tyson Hamilton, Udi Meiri,
Valentyn Tymofieiev, viktorjonsson, Xinyu Liu, Yichi Zhang, Yixing Zhang, yoshiki.obata,
Yueyang Qiu, zijiesong