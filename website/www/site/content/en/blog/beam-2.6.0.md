---
title:  "Apache Beam 2.6.0"
date:   2018-08-10 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/08/10/beam-2.6.0.html
authors:
        - pabloem
        - rfernand

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

We are glad to present the new 2.6.0 release of Beam.
This release includes multiple fixes and new functionality, such as new features in SQL and portability.<!--more-->
We also spent a significant amount of time automating the release and fixing continuous integration. For more information, check the
[release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12343392
).

## New Features / Improvements

### gRPC/Protobuf shading

* `gRPC/protobuf` is now shaded in the majority of Apache Beam
Java modules. A few modules which expose `gRPC/protobuf` on the
API surface still maintain a direct dependency.

### Beam SQL

* Added support for the `EXISTS` and `LIKE` operators.
* Implemented `SUM()` aggregations.
* Fixed issues with the `CASE` expression.
* Added support for date comparisons.
* Added unbounded data support to `LIMIT`.

### Portability

* Shared libraries for supporting timers and user state
are now available for runner integration.
* Added a Universal Local Runner, which works on a single machine using portability and containerized SDK harnesses.
* The Flink Runner now accepts jobs using the Job API.

### IOs

* Bounded `SplittableDoFn` (SDF) support is now available in all
runners (SDF is the new I/O connector API).
* `HBaseIO` is the first I/O supporting Bounded SDF (using
  `readAll`).

### SDKs

* Improved Python `AvroIO` performance.
* Python `AvroIO` has a `use_fastavro` option that uses
`fastavro` instead of `apache/avro`, for a
[3-6x speedup](https://gist.github.com/ryan-williams/ede5ae61605e7ba6aa655071858ef52b)!

### Other

* Updated various dependency versions.
* Improvements to stability, performance, and documentation.

## List of Contributors

According to git shortlog, the following 39 people contributed
to the 2.6.0 release. Thank you to all contributors!

Ahmet Altay, Alan Myrvold, Alexey Romanenko, Andrew Pilloud,
Ankur Goenka, Boyuan Zhang, Charles Chen, cclauss,
Daniel Oliveira, Elliott Brossard, Eric Beach,
Etienne Chauchot, Eugene Kirpichov, Henning Rohde,
Ismaël Mejía, Kai Jiang, Kasia, Kenneth Knowles, Luis Osa,
Lukasz Cwik, Maria Garcia Herrero, Mark Liu, Matthias Feys,
Pablo Estrada, Rafael Fernandez, Reuven Lax, Robert Bradshaw,
Robert Burke, Robin Qiu, Ryan Williams, Scott Wegner, Rui Weng,
Sergei Lebedev, Sindy Li, Thomas Weise, Udi Meiri,
Valentyn Tymofieiev, XuMingmin, and Yifan Zou.
