---
title:  "Apache Beam 2.70.0"
date:   2025-12-16 15:00:00 -0500
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

We are happy to present the new 2.70.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2700-2025-12-16) for this release.

<!--more-->

For more information on changes in 2.70.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/38?closed=1).

## Highlights

* Flink 1.20 support added ([#32647](https://github.com/apache/beam/issues/32647)).

### New Features / Improvements

* Python examples added for Milvus search enrichment handler on [Beam Website](https://beam.apache.org/documentation/transforms/python/elementwise/enrichment-milvus/)
  including jupyter notebook example (Python) ([#36176](https://github.com/apache/beam/issues/36176)).
* Milvus sink I/O connector added (Python) ([#36702](https://github.com/apache/beam/issues/36702)).
  Now Beam has full support for Milvus integration including Milvus enrichment and sink operations.

### Breaking Changes

* (Python) Some Python dependencies have been split out into extras. To ensure all previously installed dependencies are installed, when installing Beam you can `pip install apache-beam[gcp,interactive,yaml,redis,hadoop,tfrecord]`, though most users will not need all of these extras ([#34554](https://github.com/apache/beam/issues/34554)).

### Deprecations

* (Python) Python 3.9 reached EOL in October 2025 and support for the language version has been removed. ([#36665](https://github.com/apache/beam/issues/36665)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.70.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Alex Chermenin, Andrew Crites, Arun Pandian, Celeste Zeng, Chamikara Jayalath, Chenzo, Claire McGinty, Danny McCormick, Derrick Williams, Dustin Rhodes, Enrique Calderon, Ian Liao, Jack McCluskey, Jessica Hsiao, Joey Tran, Karthik Talluri, Kenneth Knowles, Maciej Szwaja, Mehdi.D, Mohamed Awnallah, Praneet Nadella, Radek Stankiewicz, Radosław Stankiewicz, Reuven Lax, RuiLong J., S. Veyrié, Sam Whittle, Shunping Huang, Stephan Hoyer, Steven van Rossum, Tanu Sharma, Tarun Annapareddy, Tom Stepp, Valentyn Tymofieiev, Vitaly Terentyev, XQ Hu, Yi Hu, changliiu, claudevdm, fozzie15, kristynsmith, wolfchris-google
