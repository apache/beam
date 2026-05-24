---
title:  "Apache Beam 2.72.0"
date:   2026-03-30 9:00:00 -0700
categories:
  - blog
  - release
authors:
  - vterentev
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

We are happy to present the new 2.72.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2720-2026-03-30) for this release.

<!--more-->

For more information on changes in 2.72.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/40).

## Highlights

* Flink 2.0 support ([#36947](https://github.com/apache/beam/issues/36947)).

### I/Os

* Add Datadog IO support (Java) ([#37318](https://github.com/apache/beam/issues/37318)).
* Remove Pubsublite IO support, since service will be deprecated in March 2026. ([#37375](https://github.com/apache/beam/issues/37375)).
* (Java) ClickHouse - migrating from the legacy JDBC driver (v0.6.3) to ClickHouse Java Client v2 (v0.9.6). See the [class documentation](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/clickhouse/ClickHouseIO.html) for migration guide   ([#37610](https://github.com/apache/beam/issues/37610)).
* (Java) Upgraded GoogleAdsIO to use GoogleAdsIO API v23 ([#37620](https://github.com/apache/beam/issues/37620)).

### New Features / Improvements

* (Python) Added exception chaining to preserve error context in CloudSQLEnrichmentHandler, processes utilities, and core transforms ([#37422](https://github.com/apache/beam/issues/37422)).
* (Python) Added a pipeline option `--experiments=pip_no_build_isolation` to disable build isolation when installing dependencies in the runtime environment ([#37331](https://github.com/apache/beam/issues/37331)).

### Deprecations

* (Python) Removed previously deprecated list_prefix method for filesystem interfaces ([#37587](https://github.com/apache/beam/issues/37587)).

### Bugfixes

* Fixed (Yaml) issue with validate compatible method ([#37588](https://github.com/apache/beam/issues/37588)).
* Fixed (Yaml) issue with Create transform dealing with different type elements ([#37585](https://github.com/apache/beam/issues/37585)).

### Security Fixes

* Fixed [CVE-2024-28397](https://www.cve.org/CVERecord?id=CVE-2024-28397) by switching from js2py to pythonmonkey (Yaml) ([#37560](https://github.com/apache/beam/issues/37560)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.72.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Andrew Crites, Arun Pandian, Ben Feinstein, Bentsi Leviav, Celeste Zeng, Danny McCormick, Danny Mccormick, Derrick Williams, Elia LIU, Ganesh, Jack McCluskey, Kenneth Knowles, Labesse Kévin, M Junaid Shaukat, Mansi Singh, Mattie Fu, Nayan Mathur, Pablo Estrada, Pirzada Ahmad Faraz, Radek Stankiewicz, Radosław Stankiewicz, Robert Bradshaw, Rohan Sah, RuiLong J., Sakthivel Subramanian, Sam Whittle, Shaheer Amjad, Shunping Huang, Steven van Rossum, Tarun Annapareddy, Tobias Kaymak, Valentyn Tymofieiev, Vitaly Terentyev, Yi Hu, XQ Hu, ZIHAN DAI, apanich, chenxuesdu, claudevdm, franzonia137
