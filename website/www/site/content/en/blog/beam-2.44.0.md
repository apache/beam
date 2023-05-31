---
title:  "Apache Beam 2.44.0"
date:   2023-01-17 09:00:00 -0700
categories:
- blog
- release
authors:
- klk
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

We are happy to present the new 2.44.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2430-2023-01-13) for this release.

<!--more-->

For more information on changes in 2.44.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/7?closed=1).

## I/Os

* Support for Bigtable sink (Write and WriteBatch) added (Go) ([#23324](https://github.com/apache/beam/issues/23324)).
* S3 implementation of the Beam filesystem (Go) ([#23991](https://github.com/apache/beam/issues/23991)).
* Support for SingleStoreDB source and sink added (Java) ([#22617](https://github.com/apache/beam/issues/22617)).
* Added support for DefaultAzureCredential authentication in Azure Filesystem (Python) ([#24210](https://github.com/apache/beam/issues/24210)).
* Added new CdapIO for CDAP Batch and Streaming Source/Sinks (Java) ([#24961](https://github.com/apache/beam/issues/24961)).
* Added new SparkReceiverIO for Spark Receivers 2.4.* (Java) ([#24960](https://github.com/apache/beam/issues/24960)).

## New Features / Improvements

* Beam now provides a portable "runner" that can render pipeline graphs with
  graphviz.  See `python -m apache_beam.runners.render --help` for more details.
* Local packages can now be used as dependencies in the requirements.txt file, rather
  than requiring them to be passed separately via the `--extra_package` option
  (Python) ([#23684](https://github.com/apache/beam/pull/23684)).
* Pipeline Resource Hints now supported via `--resource_hints` flag (Go) ([#23990](https://github.com/apache/beam/pull/23990)).
* Make Python SDK containers reusable on portable runners by installing dependencies to temporary venvs ([BEAM-12792](https://issues.apache.org/jira/browse/BEAM-12792)).
* RunInference model handlers now support the specification of a custom inference function in Python ([#22572](https://github.com/apache/beam/issues/22572))
* Support for `map_windows` urn added to Go SDK ([#24307](https://github.apache/beam/pull/24307)).

## Breaking Changes

* `ParquetIO.withSplit` was removed since splittable reading has been the default behavior since 2.35.0. The effect of
  this change is to drop support for non-splittable reading (Java)([#23832](https://github.com/apache/beam/issues/23832)).
* `beam-sdks-java-extensions-google-cloud-platform-core` is no longer a
  dependency of the Java SDK Harness. Some users of a portable runner (such as Dataflow Runner v2)
  may have an undeclared dependency on this package (for example using GCS with
  TextIO) and will now need to declare the dependency.
* `beam-sdks-java-core` is no longer a dependency of the Java SDK Harness. Users of a portable
  runner (such as Dataflow Runner v2) will need to provide this package and its dependencies.
* Slices now use the Beam Iterable Coder. This enables cross language use, but breaks pipeline updates
  if a Slice type is used as a PCollection element or State API element. (Go)[#24339](https://github.com/apache/beam/issues/24339)

## Bugfixes

* Fixed JmsIO acknowledgment issue (Java) ([#20814](https://github.com/apache/beam/issues/20814))
* Fixed Beam SQL CalciteUtils (Java) and Cross-language JdbcIO (Python) did not support JDBC CHAR/VARCHAR, BINARY/VARBINARY logical types ([#23747](https://github.com/apache/beam/issues/23747), [#23526](https://github.com/apache/beam/issues/23526)).
* Ensure iterated and emitted types are used with the generic register package are registered with the type and schema registries.(Go) ([#23889](https://github.com/apache/beam/pull/23889))

## List of Contributors

According to git shortlog, the following people contributed to the 2.44.0 release. Thank you to all contributors!

Ahmed Abualsaud

Ahmet Altay

Alex Merose

Alexey Inkin

Alexey Romanenko

Anand Inguva

Andrei Gurau

Andrej Galad

Andrew Pilloud

Ayush Sharma

Benjamin Gonzalez

Bjorn Pedersen

Brian Hulette

Bruno Volpato

Bulat Safiullin

Chamikara Jayalath

Chris Gavin

Damon Douglas

Danielle Syse

Danny McCormick

Darkhan Nausharipov

David Cavazos

Dmitry Repin

Doug Judd

Elias Segundo Antonio

Evan Galpin

Evgeny Antyshev

Heejong Lee

Henrik Heggelund-Berg

Israel Herraiz

Jack McCluskey

Jan Lukavský

Janek Bevendorff

Johanna Öjeling

John J. Casey

Jozef Vilcek

Kanishk Karanawat

Kenneth Knowles

Kiley Sok

Laksh

Liam Miller-Cushon

Luke Cwik

MakarkinSAkvelon

Minbo Bae

Moritz Mack

Nancy Xu

Ning Kang

Nivaldo Tokuda

Oleh Borysevych

Pablo Estrada

Philippe Moussalli

Pranav Bhandari

Rebecca Szper

Reuven Lax

Rick Smit

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Ryan Thompson

Sam Whittle

Sanil Jain

Scott Strong

Shubham Krishna

Steven van Rossum

Svetak Sundhar

Thiago Nunes

Tianyang Hu

Trevor Gevers

Valentyn Tymofieiev

Vitaly Terentyev

Vladislav Chunikhin

Xinyu Liu

Yi Hu

Yichi Zhang

AdalbertMemSQL

agvdndor

andremissaglia

arne-alex

bullet03

camphillips22

capthiron

creste

fab-jul

illoise

kn1kn1

nancyxu123

peridotml

shinannegans

smeet07
