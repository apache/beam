---
title:  "Apache Beam 2.71.0"
date:   2026-01-13 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - damccorm
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

We are happy to present the new 2.71.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/{TODO - replace with $DOWNLOAD_ANCHOR}) for this release.

<!--more-->

For more information on changes in 2.71.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/39).

## I/Os

* (Java) Elasticsearch 9 Support ([#36491](https://github.com/apache/beam/issues/36491)).
* (Java) Upgraded HCatalogIO to Hive 4.0.1 ([#32189](https://github.com/apache/beam/issues/32189)).

## New Features / Improvements

* Support configuring Firestore database on ReadFn transforms (Java) ([#36904](https://github.com/apache/beam/issues/36904)).
* (Python) Inference args are now allowed in most model handlers, except where they are explicitly/intentionally disallowed ([#37093](https://github.com/apache/beam/issues/37093)).

## Bugfixes

* Fixed FirestoreV1 Beam connectors allow configuring inconsistent project/database IDs between RPC requests and routing headers #36895 (Java) ([#36895](https://github.com/apache/beam/issues/36895)).
* Logical type and coder registry are saved for pipelines in the case of default pickler. This fixes a side effect of switching to cloudpickle as default pickler in Beam 2.65.0 (Python) ([#35738](https://github.com/apache/beam/issues/35738)).

## Known Issues

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.71.0 release. Thank you to all contributors!

Abacn, Ahmed Abualsaud, Amar3tto, Andrew Crites, apanich, Arun, Arun Pandian, assaf127, Chamikara Jayalath, CherisPatelInfocusp, Cheskel Twersky, Claire McGinty, Claude, Danny Mccormick, dependabot[bot], Derrick Williams, Egbert van der Wal, Evan Galpin, Ganesh, github-actions[bot], hekk-kaori-maeda, Jack Dingilian, Jack McCluskey, JayajP, Jiang Zhu, Kenneth Knowles, liferoad, M Junaid Shaukat, Nayan Mathur, Noah Stapp, Paco Avila, Radek Stankiewicz, Radosław Stankiewicz, Robert Stupp, Sam Whittle, Shunping Huang, Steven van Rossum, Suvrat Acharya, Tarun Annapareddy, tvalentyn, Utkarsh Parekh, Vitaly Terentyev, Xiaochu Liu, Yala Huang Feng, Yi Hu, Yu Watanabe, zhan7236