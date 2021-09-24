---
title:  "Apache Beam 2.33.0"
date:   2021-09-TODO 00:00:01 -0800
categories:
  - blog
authors:
  - udim
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

We are happy to present the new 2.33.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2330-2021-09-TODO) for this release.

<!--more-->

For more information on changes in 2.33.0, check out the [detailed release
notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12350404).

## Highlights

* Go SDK is no longer experimental, and is officially part of the Beam release process.
  * Matching Go SDK containers are published on release.
  * Batch usage is well supported, and tested on Flink, Spark, and the Python Portable Runner.
    * SDK Tests are also run against Google Cloud Dataflow, but this doesn't indicate reciprical support.
  * The SDK supports Splittable DoFns, Cross Language transforms, and most Beam Model basics.
  * Go Modules are now used for dependency management.
    * This is a breaking change, see Breaking Changes for resolution.
    * Easier path to contribute to the Go SDK, no need to set up a GO\_PATH.
    * Minimum Go version is now Go v1.16
  * See the announcement blogpost for full information (TODO(lostluck): Add link once published.)

<!--
{$TOPICS e.g.:}
### I/Os
* Support for X source added (Java) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
{$TOPICS}
-->

### New Features / Improvements

* Projection pushdown in SchemaIO ([BEAM-12609](https://issues.apache.org/jira/browse/BEAM-12609)).
* Upgrade Flink runner to Flink versions 1.13.2, 1.12.5 and 1.11.4 ([BEAM-10955](https://issues.apache.org/jira/browse/BEAM-10955)).

### Breaking Changes

* Python GBK by default will fail on unbounded PCollections that have global windowing and a default trigger. The `--allow_unsafe_triggers` flag can be used to override this. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Python GBK will fail if it detects an unsafe trigger unless the `--allow_unsafe_triggers` flag is set. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Since release 2.30.0, "The AvroCoder changes for BEAM-2303 \[changed\] the reader/writer from the Avro ReflectDatum* classes to the SpecificDatum* classes" (Java). The `useReflectApi` setting, introduced in this release, allows reverting to the previous behavior ([BEAM-12628](https://issues.apache.org/jira/browse/BEAM-12628)). 

<!--
### Deprecations

* X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
-->

### Bugfixes

* UnsupportedOperationException when reading from BigQuery tables and converting
  TableRows to Beam Rows (Java)
  ([BEAM-12479](https://issues.apache.org/jira/browse/BEAM-12479)).
* SDFBoundedSourceReader behaves much slower compared with the original behavior
  of BoundedSource (Python)
  ([BEAM-12781](https://issues.apache.org/jira/browse/BEAM-12781)).
* ORDER BY column not in SELECT crashes (ZetaSQL)
  ([BEAM-12759](https://issues.apache.org/jira/browse/BEAM-12759)).

### Known Issues

* The Python SDK's `allow_unsafe_triggers` check may incorrectly classify triggers wrapped in `Repeatedly` as unsafe. The workaround is to pass the `--allow_unsafe_triggers` option to the pipeline (a warning will still be issued) ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Spark 2.x users will need to update Spark's Jackson runtime dependencies (`spark.jackson.version`) to at least version 2.9.2, due to Beam updating its dependencies.
* See a full list of open [issues that affect](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20affectedVersion%20%3D%202.33.0%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC) this version.


## List of Contributors

According to git shortlog, the following people contributed to the 2.33.0 release. Thank you to all contributors!

Ahmet Altay,
Alex Amato,
Alexey Romanenko,
Andreas Bergmeier,
Andres Rodriguez,
Andrew Pilloud,
Andy Xu,
Ankur Goenka,
anthonyqzhu,
Benjamin Gonzalez,
Bhupinder Sindhwani,
Chamikara Jayalath,
Claire McGinty,
Daniel Mateus Pires,
Daniel Oliveira,
David Huntsperger,
Dylan Hercher,
emily,
Emily Ye,
Etienne Chauchot,
Eugene Nikolaiev,
Heejong Lee,
iindyk,
Iñigo San Jose Visiers,
Ismaël Mejía,
Jack McCluskey,
Jan Lukavský,
Jeff Ruane,
Jeremy Lewi,
KevinGG,
Ke Wu,
Kyle Weaver,
lostluck,
Luke Cwik,
Marwan Tammam,
masahitojp,
Mehdi Drissi,
Minbo Bae,
Ning Kang,
Pablo Estrada,
Pascal Gillet,
Pawas Chhokra,
Reuven Lax,
Ritesh Ghorse,
Robert Bradshaw,
Robert Burke,
Rodrigo Benenson,
Ryan Thompson,
Saksham Gupta,
Sam Rohde,
Sam Whittle,
Sayat,
Sayat Satybaldiyev,
Siyuan Chen,
Slava Chernyak,
Steve Niemitz,
Steven Niemitz,
tvalentyn,
Tyson Hamilton,
Udi Meiri,
vachan-shetty,
Venkatramani Rajgopal,
Yichi Zhang,
zhoufek

