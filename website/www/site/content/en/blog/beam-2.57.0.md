---
title:  "Apache Beam 2.57.0"
date:   2024-06-26 13:00:00 -0800
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

We are happy to present the new 2.57.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2570-2024-06-26) for this release.

<!--more-->

For more information on changes in 2.57.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/21).

## Highlights

* Apache Beam adds Python 3.12 support ([#29149](https://github.com/apache/beam/issues/29149)).
* Added FlinkRunner for Flink 1.18 ([#30789](https://github.com/apache/beam/issues/30789)).

## I/Os

* Ensure that BigtableIO closes the reader streams ([#31477](https://github.com/apache/beam/issues/31477)).

## New Features / Improvements

* Added Feast feature store handler for enrichment transform (Python) ([#30957](https://github.com/apache/beam/issues/30964)).
* BigQuery per-worker metrics are reported by default for Streaming Dataflow Jobs (Java) ([#31015](https://github.com/apache/beam/pull/31015))
* Adds `inMemory()` variant of Java List and Map side inputs for more efficient lookups when the entire side input fits into memory.
* Beam YAML now supports the jinja templating syntax.
  Template variables can be passed with the (json-formatted) `--jinja_variables` flag.
* DataFrame API now supports pandas 2.1.x and adds 12 more string functions for Series.([#31185](https://github.com/apache/beam/pull/31185)).
* Added BigQuery handler for enrichment transform (Python) ([#31295](https://github.com/apache/beam/pull/31295))
* Disable soft delete policy when creating the default bucket for a project (Java) ([#31324](https://github.com/apache/beam/pull/31324)).
* Added `DoFn.SetupContextParam` and `DoFn.BundleContextParam` which can be used
  as a python `DoFn.process`, `Map`, or `FlatMap` parameter to invoke a context
  manager per DoFn setup or bundle (analogous to using `setup`/`teardown`
  or `start_bundle`/`finish_bundle` respectively.)
* Go SDK Prism Runner
  * Pre-built Prism binaries are now part of the release and are available via the Github release page. ([#29697](https://github.com/apache/beam/issues/29697)).
  * ProcessingTime is now handled synthetically with TestStream pipelines and Non-TestStream pipelines, for fast test pipeline execution by default. ([#30083](https://github.com/apache/beam/issues/30083)).
    * Prism does NOT yet support "real time" execution for this release.
* Improve processing for large elements to reduce the chances for exceeding 2GB protobuf limits (Python)([https://github.com/apache/beam/issues/31607]).

## Breaking Changes

* Java's View.asList() side inputs are now optimized for iterating rather than
  indexing when in the global window.
  This new implementation still supports all (immutable) List methods as before,
  but some of the random access methods like get() and size() will be slower.
  To use the old implementation one can use View.asList().withRandomAccess().
* SchemaTransforms implemented with TypedSchemaTransformProvider now produce a
  configuration Schema with snake_case naming convention
  ([#31374](https://github.com/apache/beam/pull/31374)). This will make the following
  cases problematic:
  * Running a pre-2.57.0 remote SDK pipeline containing a 2.57.0+ Java SchemaTransform,
    and vice versa:
  * Running a 2.57.0+ remote SDK pipeline containing a pre-2.57.0 Java SchemaTransform
  * All direct uses of Python's [SchemaAwareExternalTransform](https://github.com/apache/beam/blob/a998107a1f5c3050821eef6a5ad5843d8adb8aec/sdks/python/apache_beam/transforms/external.py#L381)
    should be updated to use new snake_case parameter names.
* Upgraded Jackson Databind to 2.15.4 (Java) ([#26743](https://github.com/apache/beam/issues/26743)).
  jackson-2.15 has known breaking changes. An important one is it imposed a buffer limit for parser.
  If your custom PTransform/DoFn are affected, refer to [#31580](https://github.com/apache/beam/pull/31580) for mitigation.

## Known Issues

* Python pipelines that run with 2.53.0-2.58.0 SDKs and read data from GCS might be affected by a data corruption issue ([#32169](https://github.com/apache/beam/issues/32169)). The issue will be fixed in 2.59.0 ([#32135](https://github.com/apache/beam/pull/32135)). To work around this, update the google-cloud-storage package to version 2.18.2 or newer.

For the most up to date list of known issues, see https://github.com/apache/beam/blob/master/CHANGES.md

## List of Contributors

According to git shortlog, the following people contributed to the 2.57.0 release. Thank you to all contributors!

Ahmed Abualsaud

Ahmet Altay

Alexey Romanenko

Andrey Devyatkin

Anody Zhang

Arvind Ram

Ben Konz

Bruno Volpato

Celeste Zeng

Chamikara Jayalath

Claire McGinty

Colm O hEigeartaigh

Damon

Danny McCormick

Evan Galpin

Ferran Fernández Garrido

Florent Biville

Jack Dingilian

Jack McCluskey

Jan Lukavský

JayajP

Jeff Kinard

Jeffrey Kinard

John Casey

Justin Uang

Kenneth Knowles

Kevin Zhou

Liam Miller-Cushon

Maarten Vercruysse

Maciej Szwaja

Maja Kontrec Rönn

Marc hurabielle

Martin Trieu

Mattie Fu

Min Zhu

Naireen Hussain

Nick Anikin

Pablo Rodriguez Defino

Paul King

Priyans Desai

Radosław Stankiewicz

Rebecca Szper

Ritesh Ghorse

Robert Bradshaw

Robert Burke

Rodrigo Bozzolo

RyuSA

Sam Rohde

Sam Whittle

Sergei Lilichenko

Shahar Epstein

Shunping Huang

Svetak Sundhar

Tomo Suzuki

Tony Tang

Valentyn Tymofieiev

Vincent Stollenwerk

Vineet Kumar

Vitaly Terentyev

Vlado Djerek

XQ Hu

Yi Hu

akashorabek

bzablocki

kberezin

