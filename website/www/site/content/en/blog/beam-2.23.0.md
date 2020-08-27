---
title:  "Apache Beam 2.23.0"
date:   2020-07-29 00:00:01 -0800
categories:
  - blog
authors:
  - Valentyn Tymofieiev
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
We are happy to present the new 2.23.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2230-2020-07-29) for this release.
For more information on changes in 2.23.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12347145).

## Highlights

* Twister2 Runner ([BEAM-7304](https://issues.apache.org/jira/browse/BEAM-7304)).
* Python 3.8 support ([BEAM-8494](https://issues.apache.org/jira/browse/BEAM-8494)).

## I/Os

* Support for reading from Snowflake added (Java) ([BEAM-9722](https://issues.apache.org/jira/browse/BEAM-9722)).
* Support for writing to Splunk added (Java) ([BEAM-8596](https://issues.apache.org/jira/browse/BEAM-8596)).
* Support for assume role added (Java) ([BEAM-10335](https://issues.apache.org/jira/browse/BEAM-10335)).
* A new transform to read from BigQuery has been added: `apache_beam.io.gcp.bigquery.ReadFromBigQuery`. This transform
  is experimental. It reads data from BigQuery by exporting data to Avro files, and reading those files. It also supports
  reading data by exporting to JSON files. This has small differences in behavior for Time and Date-related fields. See
  Pydoc for more information.
* Add dispositions for SnowflakeIO.write ([BEAM-10343](https://issues.apache.org/jira/browse/BEAM-10343))

## New Features / Improvements

* Update Snowflake JDBC dependency and add application=beam to connection URL ([BEAM-10383](https://issues.apache.org/jira/browse/BEAM-10383)).

## Breaking Changes

* `RowJson.RowJsonDeserializer`, `JsonToRow`, and `PubsubJsonTableProvider` now accept "implicit
  nulls" by default when deserializing JSON (Java) ([BEAM-10220](https://issues.apache.org/jira/browse/BEAM-10220)).
  Previously nulls could only be represented with explicit null values, as in
  `{"foo": "bar", "baz": null}`, whereas an implicit null like `{"foo": "bar"}` would raise an
  exception. Now both JSON strings will yield the same result by default. This behavior can be
  overridden with `RowJson.RowJsonDeserializer#withNullBehavior`.
* Fixed a bug in `GroupIntoBatches` experimental transform in Python to actually group batches by key.
  This changes the output type for this transform ([BEAM-6696](https://issues.apache.org/jira/browse/BEAM-6696)).

## Deprecations

* Remove Gearpump runner. ([BEAM-9999](https://issues.apache.org/jira/browse/BEAM-9999))
* Remove Apex runner. ([BEAM-9999](https://issues.apache.org/jira/browse/BEAM-9999))
* RedisIO.readAll() is deprecated and will be removed in 2 versions, users must use RedisIO.readKeyPatterns() as a replacement ([BEAM-9747](https://issues.apache.org/jira/browse/BEAM-9747)).

## Known Issues

## List of Contributors

According to git shortlog, the following people contributed to the 2.23.0 release. Thank you to all contributors!

Aaron, Abhishek Yadav, Ahmet Altay, aiyangar, Aizhamal Nurmamat kyzy, Ajo Thomas, Akshay-Iyangar, Alan Pryor, Alex Amato, Alexey Romanenko, Allen Pradeep Xavier, Andrew Crites, Andrew Pilloud, Ankur Goenka, Anna Qin, Ashwin Ramaswami, bntnam, Borzoo Esmailloo, Boyuan Zhang, Brian Hulette, Brian Michalski, brucearctor, Chamikara Jayalath, chi-chi weng, Chuck Yang, Chun Yang, Colm O hEigeartaigh, Corvin Deboeser, Craig Chambers, Damian Gadomski, Damon Douglas, Daniel Oliveira, Dariusz Aniszewski, darshanj, darshan jani, David Cavazos, David Moravek, David Yan, Esun Kim, Etienne Chauchot, Filipe Regadas, fuyuwei, Graeme Morgan, Hannah-Jiang, Harch Vardhan, Heejong Lee, Henry Suryawirawan, InigoSJ, Ismaël Mejía, Israel Herraiz, Jacob Ferriero, Jan Lukavský, Jie Fan, John Mora, Jozef Vilcek, Julien Phalip, Justine Koa, Kamil Gabryjelski, Kamil Wasilewski, Kasia Kucharczyk, Kenneth Jung, Kenneth Knowles, kevingg, Kevin Sijo Puthusseri, kshivvy, Kyle Weaver, Kyoungha Min, Kyungwon Jo, Luke Cwik, Mark Liu, Mark-Zeng, Matthias Baetens, Maximilian Michels, Michal Walenia, Mikhail Gryzykhin, Nam Bui, Nathan Fisher, Niel Markwick, Ning Kang, Omar Ismail, Pablo Estrada, paul fisher, Pawel Pasterz, perkss, Piotr Szuberski, pulasthi, purbanow, Rahul Patwari, Rajat Mittal, Rehman, Rehman Murad Ali, Reuben van Ammers, Reuven Lax, Reza Rokni, Rion Williams, Robert Bradshaw, Robert Burke, Rui Wang, Ruoyun Huang, sabhyankar, Sam Rohde, Sam Whittle, sclukas77, Sebastian Graca, Shoaib Zafar, Sruthi Sree Kumar, Stephen O'Kennedy, Steve Koonce, Steve Niemitz, Steven van Rossum, Ted Romer, Tesio, Thinh Ha, Thomas Weise, Tobias Kaymak, tobiaslieber-cognitedata, Tobiasz Kędzierski, Tomo Suzuki, Tudor Marian, tvs, Tyson Hamilton, Udi Meiri, Valentyn Tymofieiev, Vasu Nori, xuelianhan, Yichi Zhang, Yifan Zou, Yixing Zhang, yoshiki.obata, Yueyang Qiu, Yu Feng, Yuwei Fu, Zhuo Peng, ZijieSong946.
