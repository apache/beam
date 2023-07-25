---
title:  "Apache Beam 2.31.0"
date:   2021-07-08 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - apilloud
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

We are happy to present the new 2.31.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2310-2021-07-08) for this release.

<!--more-->

For more information on changes in 2.31.0, check out the [detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12349991).

## Highlights

### I/Os

* Fixed bug in ReadFromBigQuery when a RuntimeValueProvider is used as value of table argument (Python) ([BEAM-12514](https://issues.apache.org/jira/browse/BEAM-12514)).

### New Features / Improvements

* `CREATE FUNCTION` DDL statement added to Calcite SQL syntax. `JAR` and `AGGREGATE` are now reserved keywords. ([BEAM-12339](https://issues.apache.org/jira/browse/BEAM-12339)).
* Flink 1.13 is now supported by the Flink runner ([BEAM-12277](https://issues.apache.org/jira/browse/BEAM-12277)).
* DatastoreIO: Write and delete operations now follow automatic gradual ramp-up,
  in line with best practices (Java/Python) ([BEAM-12260](https://issues.apache.org/jira/browse/BEAM-12260), [BEAM-12272](https://issues.apache.org/jira/browse/BEAM-12272)).
* Python `TriggerFn` has a new `may_lose_data` method to signal potential data loss. Default behavior assumes safe (necessary for backwards compatibility). See Deprecations for potential impact of overriding this. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

### Breaking Changes

* Python Row objects are now sensitive to field order. So `Row(x=3, y=4)` is no
  longer considered equal to `Row(y=4, x=3)` (BEAM-11929).
* Kafka Beam SQL tables now ascribe meaning to the LOCATION field; previously
  it was ignored if provided.
* `TopCombineFn` disallow `compare` as its argument (Python) ([BEAM-7372](https://issues.apache.org/jira/browse/BEAM-7372)).
* Drop support for Flink 1.10 ([BEAM-12281](https://issues.apache.org/jira/browse/BEAM-12281)).

### Deprecations

* Python GBK will stop supporting unbounded PCollections that have global windowing and a default trigger in Beam 2.33. This can be overriden with `--allow_unsafe_triggers`. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Python GBK will start requiring safe triggers or the `--allow_unsafe_triggers` flag starting with Beam 2.33. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

### Known Issues

* See a full list of [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.31.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.

## List of Contributors

According to `git shortlog`, the following people contributed to the 2.31.0 release. Thank you to all contributors!

Ahmet Altay, ajo thomas, Alan Myrvold, Alex Amato, Alexey Romanenko,
AlikRodriguez, Anant Damle, Andrew Pilloud, Benjamin Gonzalez, Boyuan Zhang,
Brian Hulette, Chamikara Jayalath, Daniel Oliveira, David Cavazos,
David Huntsperger, David Moravek, Dmytro Kozhevin, dpcollins-google, Emily Ye,
Ernesto Valentino, Evan Galpin, Fernando Morales, Heejong Lee, Ismaël Mejía,
Jan Lukavský, Josias Rico, jrynd, Kenneth Knowles, Ke Wu, kileys, Kyle Weaver,
masahitojp, Matthias Baetens, Maximilian Michels, Milena Bukal,
Nathan J. Mehl, Pablo Estrada, Peter Sobot, Reuven Lax, Robert Bradshaw,
Robert Burke, roger-mike, Sam Rohde, Sam Whittle, Stephan Hoyer, Tom Underhill,
tvalentyn, Uday Singh, Udi Meiri, Vitaly Terentyev, Xinyu Liu, Yichi Zhang,
Yifan Mai, yoshiki.obata, zhoufek

